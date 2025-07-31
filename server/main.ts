// deno-lint-ignore-file require-await
import { withRuntime } from "@deco/workers-runtime";
import {
  createStepFromTool,
  createTool,
  createWorkflow,
} from "@deco/workers-runtime/mastra";
import { z } from "zod";
import { type Env as DecoEnv, Policies, StateSchema } from "./deco.gen.ts";

interface Env extends DecoEnv {
  ASSETS: {
    fetch: (request: Request) => Promise<Response>;
  };
}

// const createMyTool = (_env: Env) =>
//   createTool({
//     id: "MY_TOOL",
//     description: "Say hello",
//     inputSchema: z.object({ name: z.string() }),
//     outputSchema: z.object({ message: z.string() }),
//     execute: async ({ context }) => ({
//       message: `Hello, ${context.name}!`,
//     }),
//   });

const createCsvToDbTool = (env: Env) =>
  createTool({
    id: "CSV_TO_DB",
    description:
      "Fetch CSV data from a URL and insert it into a database table",
    inputSchema: z.object({
      csvUrl: z.string().url().describe("URL to the CSV file"),
      tableName: z.string().describe(
        "Name of the database table to insert data into",
      ),
      createTable: z.boolean().optional().default(false).describe(
        "Whether to create the table if it doesn't exist",
      ),
    }),
    outputSchema: z.object({
      success: z.boolean(),
      rowsInserted: z.number(),
      message: z.string(),
      tableCreated: z.boolean().optional(),
    }),
    execute: async ({ context }) => {
      try {
        // Fetch CSV data from URL
        const csvResponse = await fetch(context.csvUrl);
        if (!csvResponse.ok) {
          throw new Error(
            `Failed to fetch CSV: ${csvResponse.status} ${csvResponse.statusText}`,
          );
        }

        const csvText = await csvResponse.text();
        if (!csvText.trim()) {
          throw new Error("CSV file is empty");
        }

        // Parse CSV data (simple parser for demonstration)
        const lines = csvText.trim().split("\n");
        if (lines.length < 2) {
          throw new Error(
            "CSV must have at least a header row and one data row",
          );
        }

        // Extract headers and data
        const headers = lines[0].split(",").map((h) =>
          h.trim().replace(/"/g, "")
        );
        const dataRows = lines.slice(1).map((line) => {
          // Simple CSV parsing - handles basic quoted fields
          const values: string[] = [];
          let currentValue = "";
          let inQuotes = false;

          for (let i = 0; i < line.length; i++) {
            const char = line[i];
            if (char === '"') {
              inQuotes = !inQuotes;
            } else if (char === "," && !inQuotes) {
              values.push(currentValue.trim());
              currentValue = "";
            } else {
              currentValue += char;
            }
          }
          values.push(currentValue.trim()); // Add the last value

          return values.map((v) => v.replace(/"/g, ""));
        });

        let tableCreated = false;

        // Create table if requested
        if (context.createTable) {
          // Generate CREATE TABLE statement based on CSV headers
          // Sanitize column names for SQLite compatibility
          const sanitizedHeaders = headers.map((header) =>
            header.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_+/g, "_").replace(
              /^_|_$/g,
              "",
            )
          );

          const columnDefinitions = sanitizedHeaders.map((header) =>
            `"${header}" TEXT`
          ).join(", ");

          const createTableSql =
            `CREATE TABLE IF NOT EXISTS "${context.tableName}" (${columnDefinitions})`;

          const createResult = await env.DECO_CHAT_WORKSPACE_API
            .DATABASES_RUN_SQL({
              sql: createTableSql,
              params: [],
            });

          if (!createResult.result[0]?.success) {
            throw new Error("Failed to create table");
          }
          tableCreated = true;
        }

        // Sanitize headers once and reuse for all operations
        const sanitizedHeaders = headers.map((header) =>
          header.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_+/g, "_").replace(
            /^_|_$/g,
            "",
          )
        );

        // Insert data into database line by line (sequential)
        let rowsInserted = 0;

        for (const row of dataRows) {
          // Ensure row has the same number of values as headers
          const values = row.slice(0, headers.length);
          while (values.length < headers.length) {
            values.push(""); // Fill missing values with empty strings
          }

          // Create single-row INSERT statement with pre-sanitized headers
          const placeholders = sanitizedHeaders.map(() => "?").join(", ");
          const singleRowInsertSql = `INSERT INTO "${context.tableName}" ("${
            sanitizedHeaders.join('", "')
          }") VALUES (${placeholders})`;

          // Execute single row insert
          const insertResult = await env.DECO_CHAT_WORKSPACE_API
            .DATABASES_RUN_SQL({
              sql: singleRowInsertSql,
              params: values,
            });

          if (insertResult.result[0]?.success) {
            rowsInserted++;
          } else {
            throw new Error(
              `Row insert failed: ${JSON.stringify(insertResult.result[0])}`,
            );
          }
        }

        return {
          success: true,
          rowsInserted,
          message:
            `Successfully inserted ${rowsInserted} rows into table '${context.tableName}'`,
          tableCreated,
        };
      } catch (error) {
        return {
          success: false,
          rowsInserted: 0,
          message: `Error: ${
            error instanceof Error ? error.message : "Unknown error occurred"
          }`,
        };
      }
    },
  });

// const createMyWorkflow = (env: Env) => {
//   const step = createStepFromTool(createMyTool(env));

//   return createWorkflow({
//     id: "MY_WORKFLOW",
//     inputSchema: z.object({ name: z.string() }),
//     outputSchema: z.object({ message: z.string() }),
//   })
//     .then(step)
//     .commit();
// };

const fallbackToView = (viewPath: string = "/") => (req: Request, env: Env) => {
  const LOCAL_URL = "http://localhost:4000";
  const url = new URL(req.url);
  const useDevServer = (req.headers.get("origin") || req.headers.get("host"))
    ?.includes("localhost");

  const request = new Request(
    useDevServer
      ? new URL(`${url.pathname}${url.search}`, LOCAL_URL)
      : new URL(viewPath, req.url),
    req,
  );

  return useDevServer ? fetch(request) : env.ASSETS.fetch(request);
};

const { Workflow, ...runtime } = withRuntime<Env, typeof StateSchema>({
  // workflows: [createMyWorkflow],
  tools: [createCsvToDbTool],
  oauth: { state: StateSchema, scopes: [Policies.GMAIL.GetEmails] },
  fetch: fallbackToView("/"),
});

export { Workflow };

export default runtime;
