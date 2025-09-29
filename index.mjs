#!/usr/bin/env node

/**
 * PostgreSQL Schema Model Context Protocol (MCP) Server (Updated for v1.18.2)
 *
 * This MCP server provides schema-only access to PostgreSQL databases.
 * It allows clients to discover and read database table schemas without
 * executing queries or modifying data.
 *
 * MCP Server Creation Workflow (v1.18.2):
 * 1. Initialize McpServer instance with name and version
 * 2. Set up database connection pool
 * 3. Register resources, tools, and prompts using the new simplified API
 * 4. Connect server to transport layer (stdio)
 * 5. Start the server
 */

import pg from "pg";
import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

// Step 1: Initialize the MCP Server with metadata
const server = new McpServer({
  name: "postgres-context-server",
  version: "0.2.0",
});

// Step 2: Set up database connection and validation
const databaseUrl = process.env.DATABASE_URL;
if (databaseUrl == null || databaseUrl.trim().length === 0) {
  console.error("Please provide a DATABASE_URL environment variable");
  process.exit(1);
}

// Create a sanitized URL for resource identification (removes password for security)
const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

process.stderr.write("starting server\n");

// Initialize PostgreSQL connection pool for efficient connection management
const pool = new pg.Pool({
  connectionString: databaseUrl,
});

// Constants for MCP operations
const SCHEMA_PATH = "schema";           // Path component for schema resources
const SCHEMA_PROMPT_NAME = "pg-schema"; // Name of the schema prompt
const ALL_TABLES = "all-tables";        // Special identifier for all tables mode

/**
 * Step 3a: Register Dynamic Schema Resources
 *
 * Using the new ResourceTemplate API to register dynamic schema resources.
 * Each database table is exposed as a resource containing its schema.
 */
server.registerResource(
  "table-schema",
  new ResourceTemplate("postgres://{host}/{tableName}/schema"),
  {
    title: "PostgreSQL Table Schema",
    description: "Schema information for PostgreSQL database tables",
    mimeType: "application/json"
  },
  async (uri, { host, tableName }) => {
    const client = await pool.connect();
    try {
      // Get column information for the specific table
      const result = await client.query(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
        [tableName],
      );

      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "application/json",
            text: JSON.stringify(result.rows, null, 2),
          },
        ],
      };
    } finally {
      client.release();
    }
  }
);

/**
 * Step 3b: Register List Resource Handler
 *
 * This provides a way to list all available table schemas.
 */
server.registerResource(
  "tables-list",
  "postgres://tables/list",
  {
    title: "Available PostgreSQL Tables",
    description: "List of all available PostgreSQL tables in the public schema",
    mimeType: "application/json"
  },
  async (uri) => {
    const client = await pool.connect();
    try {
      // Query all tables in the public schema
      const result = await client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
      );
      
      const tables = result.rows.map((row) => ({
        table_name: row.table_name,
        schema_uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href
      }));

      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "application/json",
            text: JSON.stringify(tables, null, 2),
          },
        ],
      };
    } finally {
      client.release();
    }
  }
);

/**
 * Step 3c: Register PostgreSQL Schema Tool
 *
 * Using the new registerTool API with Zod schema validation.
 * This tool generates formatted SQL CREATE TABLE statements.
 */
server.registerTool(
  "pg-schema",
  {
    title: "PostgreSQL Schema Tool",
    description: "Returns the schema for a Postgres database table or all tables",
    inputSchema: z.object({
      mode: z.enum(["all", "specific"]).describe("Mode of schema retrieval"),
      tableName: z.string().optional().describe("Name of the specific table (required if mode is 'specific')")
    }).refine(
      (data) => data.mode !== "specific" || data.tableName,
      {
        message: "tableName is required when mode is 'specific'",
        path: ["tableName"]
      }
    )
  },
  async ({ mode, tableName }) => {
    const client = await pool.connect();

    try {
      // Determine which table(s) to process based on mode
      const targetTable = mode === "specific" ? tableName : ALL_TABLES;
      
      if (mode === "specific" && (!tableName || tableName.trim().length === 0)) {
        throw new Error("Invalid tableName: tableName is required for 'specific' mode");
      }

      // Generate formatted SQL schema using utility function
      const sql = await getSchema(client, targetTable);

      return {
        content: [{ type: "text", text: sql }],
      };
    } finally {
      client.release();
    }
  }
);

/**
 * Step 3d: Register PostgreSQL Schema Prompt
 *
 * Using the new registerPrompt API for generating schema prompts.
 */
server.registerPrompt(
  SCHEMA_PROMPT_NAME,
  {
    title: "PostgreSQL Schema Prompt",
    description: "Retrieve the schema for a given table in the postgres database",
    arguments: [
      {
        name: "tableName",
        description: "The table to describe (or 'all-tables' for all tables)",
        required: true,
      },
    ],
  },
  async ({ tableName }) => {
    if (typeof tableName !== "string" || tableName.length === 0) {
      throw new Error(`Invalid tableName: ${tableName}`);
    }

    const client = await pool.connect();

    try {
      // Generate schema SQL for the requested table(s)
      const sql = await getSchema(client, tableName);

      return {
        description:
          tableName === ALL_TABLES
            ? "All PostgreSQL table schemas"
            : `PostgreSQL schema for table: ${tableName}`,
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: sql,
            },
          },
        ],
      };
    } finally {
      client.release();
    }
  }
);

/**
 * Utility Function: getSchema
 *
 * Generates formatted SQL CREATE TABLE statements for database tables.
 * This function queries the PostgreSQL information_schema to retrieve
 * column definitions and formats them as human-readable SQL.
 *
 * @param {object} client - PostgreSQL client connection
 * @param {string} tableNameOrAll - Either a specific table name or "all-tables"
 * @returns {string} Formatted SQL CREATE TABLE statements wrapped in markdown
 */
async function getSchema(client, tableNameOrAll) {
  const select =
    "SELECT column_name, data_type, is_nullable, column_default, table_name FROM information_schema.columns";

  let result;
  if (tableNameOrAll === ALL_TABLES) {
    // Get columns for all user tables (exclude system schemas)
    result = await client.query(
      `${select} WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name, ordinal_position`,
    );
  } else {
    // Get columns for a specific table
    result = await client.query(`${select} WHERE table_name = $1 ORDER BY ordinal_position`, [
      tableNameOrAll,
    ]);
  }

  if (result.rows.length === 0) {
    if (tableNameOrAll === ALL_TABLES) {
      return "```sql\n-- No tables found in the database\n```";
    } else {
      return `\`\`\`sql\n-- Table '${tableNameOrAll}' not found\n\`\`\``;
    }
  }

  // Extract unique table names and sort them
  const allTableNames = Array.from(
    new Set(result.rows.map((row) => row.table_name).sort()),
  );

  // Build formatted SQL output
  let sql = "```sql\n";
  for (let i = 0, len = allTableNames.length; i < len; i++) {
    const tableName = allTableNames[i];
    if (i > 0) {
      sql += "\n"; // Add spacing between tables
    }

    // Generate CREATE TABLE statement for this table
    sql += [
      `CREATE TABLE "${tableName}" (`,
      result.rows
        .filter((row) => row.table_name === tableName)
        .map((row) => {
          // Format column definition with data type, nullability, and defaults
          const notNull = row.is_nullable === "NO" ? " NOT NULL" : "";
          const defaultValue =
            row.column_default != null ? ` DEFAULT ${row.column_default}` : "";
          return `    "${row.column_name}" ${row.data_type}${notNull}${defaultValue}`;
        })
        .join(",\n"),
      ");",
    ].join("\n");
    sql += "\n";
  }
  sql += "```";

  return sql;
}

/**
 * Step 4: Server Startup
 *
 * Connect the MCP server to its transport layer and start listening for requests.
 * Uses stdio transport for communication with MCP clients.
 */
async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write("PostgreSQL MCP server started successfully\n");
}

// Step 5: Start the server with error handling
runServer().catch((error) => {
  console.error("Failed to start MCP server:", error);
  process.exit(1);
});

// Graceful shutdown handling
process.on('SIGINT', async () => {
  process.stderr.write("Shutting down server...\n");
  await pool.end();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  process.stderr.write("Shutting down server...\n");
  await pool.end();
  process.exit(0);
});