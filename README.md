## Use

Copy the example config file, `config/example.yaml`.
Copy the example message processor, `config/message-processors-example.ts`.

Set AWS profile (optional):

`export AWS_PROFILE=myprofile`

List queues:

`npm run redrive -- -c config/example.yaml -l`

Receive messages:

`npm run redrive -- -c config/example.yaml -r`

Process messages:

`npm run redrive -- -c config/example.yaml -p`

Send messages:

`npm run redrive -- -c config/example.yaml -s`

## Directory Structure

```
/messages (`dataDirectory`)
  /<queue name>
      /deletes
        /pending (place messages in here to delete the messages from the source queue)
        /archived (messages placed here on success)
        /errors (messages placed here on failure)
      /processing-errors (stores errors from the processing step)
        <message id>.json
      /received (original messages from --receive)
        <message id>.json
      /skips (place messages here to skips)
        <message id>.json
      /updates
        /pending (place messages here to send and delete from the source queue)
        /archived (messages placed here on success)
        /errors (messages placed here on failure)
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
export AWS_PROFILE=<profile>
```
