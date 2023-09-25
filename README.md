## Use

Copy the example config file, `config/example.yaml`.

Set AWS profile (optional):

`export AWS_PROFILE=myprofile`

List queues:

`npm run redrive -- -c config/example.yaml -l`

Receive messages:

`npm run redrive -- -c config/example.yaml -r`

## Directory Structure

```
/messages (`dataDirectory`)
  /<queue name>
      /received (original messages from --receive)
        <message id>.json
      /updates (place messages in here to send message to destination queue and delete them from the source queue)
      /delete (place messages in here to delete the messages from the source queue)
      /skip (place messages here to skip)
```

## Debugging

Example `.vscode/launch.json`:

```
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "node",
      "request": "launch",
      "name": "Launch Program",
      "skipFiles": ["<node_internals>/**"],
      "program": "${workspaceFolder}/src/app.ts",
      "preLaunchTask": "tsc: build - tsconfig.json",
      "outFiles": ["${workspaceFolder}/out/**/*.js"],
      "args": ["--receive", "--config", "example.yaml"]
    }
  ]
}
```

Create a file `.env` with:

```
export AWS_PROFILE=<env>
```
