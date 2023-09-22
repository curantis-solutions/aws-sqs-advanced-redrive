import fs from "fs";
import YAML from "yaml";
import { Config } from "./models/config";

export function getConfig(path: string): Config {
  const file = fs.readFileSync(path, "utf8");
  return YAML.parse(file);
}
