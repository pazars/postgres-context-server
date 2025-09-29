#!/usr/bin/env node

/**
 * PostgreSQL Schema Model Context Protocol (MCP) Server
 *
 * This MCP server provides schema-only access to PostgreSQL databases.
 * It allows clients to discover and read database table schemas without
 * executing queries or modifying data.
 *
 * MCP Server Creation Workflow:
 * 1. Initialize Server instance with name and version
 * 2. Set up database connection pool
 * 3. Register request handlers for different MCP operations:
 *    - ListResources: Discover available database tables
 *    - ReadResource: Get schema information for specific tables
 *    - ListTools: Advertise available tools (pg-schema)
 *    - CallTool: Execute the pg-schema tool
 *    - ListPrompts: Advertise available prompts
 *    - GetPrompt: Execute prompts for schema retrieval
 *    - Complete: Provide auto-completion for table names
 * 4. Connect server to transport layer (stdio)
 * 5. Start the server
 */

import pg from "pg";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListPromptsRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
  GetPromptRequestSchema,
  CompleteRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// Step 1: Initialize the MCP Server with metadata
const server = new Server({
  name: "postgres-context-server",
  version: "0.1.0",
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
 * Step 3a: ListResources Handler
 *
 * This handler responds to requests for available resources.
 * In MCP, resources are addressable pieces of content that can be read.
 * Here, each database table is exposed as a resource containing its schema.
 *
 * Returns: Array of resource objects with URI, mimeType, and human-readable name
 */
server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    // Query all tables in the public schema
    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    );
    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    client.release();
  }
});

/**
 * Step 3b: ReadResource Handler
 *
 * This handler reads the content of a specific resource identified by URI.
 * It parses the URI to extract the table name and returns the table's
 * column information as JSON.
 *
 * URI format: postgres://host/table_name/schema
 * Returns: JSON containing column names and data types
 */
server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  // Parse URI to extract table name and validate path
  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

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
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

/**
 * Step 3c: ListTools Handler
 *
 * This handler advertises the tools available to MCP clients.
 * Tools are executable functions that clients can call to perform operations.
 * Here we expose the "pg-schema" tool for retrieving database schemas.
 *
 * Returns: Array of tool definitions with names, descriptions, and input schemas
 */
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "pg-schema",
        description: "Returns the schema for a Postgres database.",
        inputSchema: {
          type: "object",
          properties: {
            mode: {
              type: "string",
              enum: ["all", "specific"],
              description: "Mode of schema retrieval",
            },
            tableName: {
              type: "string",
              description:
                "Name of the specific table (required if mode is 'specific')",
            },
          },
          required: ["mode"],
          // Conditional validation: tableName required when mode is "specific"
          if: {
            properties: { mode: { const: "specific" } },
          },
          then: {
            required: ["tableName"],
          },
        },
      },
    ],
  };
});

/**
 * Step 3d: CallTool Handler
 *
 * This handler executes tools when called by MCP clients.
 * It processes the "pg-schema" tool to generate formatted SQL schema
 * representations for either specific tables or all tables.
 *
 * Returns: Text content containing SQL CREATE TABLE statements
 */
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "pg-schema") {
    const mode = request.params.arguments?.mode;

    // Determine which table(s) to process based on mode
    const tableName = (() => {
      switch (mode) {
        case "specific": {
          const tableName = request.params.arguments?.tableName;

          if (typeof tableName !== "string" || tableName.length === 0) {
            throw new Error(`Invalid tableName: ${tableName}`);
          }

          return tableName;
        }
        case "all": {
          return ALL_TABLES;
        }
        default:
          throw new Error(`Invalid mode: ${mode}`);
      }
    })();

    const client = await pool.connect();

    try {
      // Generate formatted SQL schema using utility function
      const sql = await getSchema(client, tableName);

      return {
        content: [{ type: "text", text: sql }],
      };
    } finally {
      client.release();
    }
  }

  throw new Error("Tool not found");
});

/**
 * Step 3e: Complete Handler
 *
 * This handler provides auto-completion suggestions for prompt arguments.
 * It helps users discover available table names when using the pg-schema prompt.
 *
 * Returns: Array of completion values (table names + "all-tables" option)
 */
server.setRequestHandler(CompleteRequestSchema, async (request) => {
  process.stderr.write("Handling completions/complete request\n");

  if (request.params.ref.name === SCHEMA_PROMPT_NAME) {
    const tableNameQuery = request.params.argument.value;
    // Check if user has already entered multiple words (completion not needed)
    const alreadyHasArg = /\S*\s/.test(tableNameQuery);

    if (alreadyHasArg) {
      return {
        completion: {
          values: [],
        },
      };
    }

    const client = await pool.connect();
    try {
      // Get all available table names for completion
      const result = await client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
      );
      const tables = result.rows.map((row) => row.table_name);
      return {
        completion: {
          values: [ALL_TABLES, ...tables], // Include "all-tables" option first
        },
      };
    } finally {
      client.release();
    }
  }

  throw new Error("unknown prompt");
});

/**
 * Step 3f: ListPrompts Handler
 *
 * This handler advertises available prompts to MCP clients.
 * Prompts are pre-defined templates that clients can use to generate
 * contextual information. Here we expose the "pg-schema" prompt.
 *
 * Returns: Array of prompt definitions with names, descriptions, and arguments
 */
server.setRequestHandler(ListPromptsRequestSchema, async () => {
  process.stderr.write("Handling prompts/list request\n");

  return {
    prompts: [
      {
        name: SCHEMA_PROMPT_NAME,
        description:
          "Retrieve the schema for a given table in the postgres database",
        arguments: [
          {
            name: "tableName",
            description: "the table to describe",
            required: true,
          },
        ],
      },
    ],
  };
});

/**
 * Step 3g: GetPrompt Handler
 *
 * This handler executes prompts and returns formatted messages for MCP clients.
 * It generates schema information as conversational context that can be
 * used in AI model interactions.
 *
 * Returns: Prompt response with description and formatted messages
 */
server.setRequestHandler(GetPromptRequestSchema, async (request) => {
  process.stderr.write("Handling prompts/get request\n");

  if (request.params.name === SCHEMA_PROMPT_NAME) {
    const tableName = request.params.arguments?.tableName;

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
            ? "all table schemas"
            : `${tableName} schema`,
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

  throw new Error(`Prompt '${request.params.name}' not implemented`);
});

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
      `${select} WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`,
    );
  } else {
    // Get columns for a specific table
    result = await client.query(`${select} WHERE table_name = $1`, [
      tableNameOrAll,
    ]);
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
      `create table "${tableName}" (`,
      result.rows
        .filter((row) => row.table_name === tableName)
        .map((row) => {
          // Format column definition with data type, nullability, and defaults
          const notNull = row.is_nullable === "NO" ? " not null" : "";
          const defaultValue =
            row.column_default != null ? ` default ${row.column_default}` : "";
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
}

// Step 5: Start the server with error handling
runServer().catch((error) => {
  console.error("Failed to start MCP server:", error);
  process.exit(1);
});
