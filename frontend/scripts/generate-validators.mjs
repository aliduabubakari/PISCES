import { readFile, writeFile } from "node:fs/promises";
import Ajv2020 from "ajv/dist/2020.js";
import standaloneCode from "ajv/dist/standalone/index.js";

for (const name of ["report-view", "campaign-view"]) {
  const schema = JSON.parse(
    await readFile(
      new URL(
        `../../src/pisces/schemas/${name}-1.0.0.schema.json`,
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const ajv = new Ajv2020({
    strict: false,
    strictNumbers: true,
    code: { source: true, esm: true },
  });
  const code =
    "// Generated from the Python-owned JSON schema. Do not edit.\n" +
    standaloneCode(ajv, ajv.compile(schema));
  // New schema features may require bundling Ajv runtime helpers before delivery.
  if (code.includes("require("))
    throw new Error("Validator requires an unbundled runtime helper");
  const path = new URL(
    `../src/api/generated/${name}.validator.js`,
    import.meta.url,
  );
  if (process.argv.includes("--check")) {
    if ((await readFile(path, "utf8")) !== code)
      throw new Error(`Stale validator: ${name}`);
  } else {
    await writeFile(path, code);
  }
}
