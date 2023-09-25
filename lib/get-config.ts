import fs from "fs";
import YAML from "yaml";
import { Config } from "./models/config";

export function getConfig(path: string): Config {
  const file = fs.readFileSync(path, "utf8");
  const config: Config = YAML.parse(file);

  // defaults
  config.dataDirectory = config.dataDirectory ?? "messages";
  config.receiveCount = config.receiveCount ?? 10;
  config.parseBody = config.parseBody ?? true;

  return config;
}
