## Concepts

The basic idea is that redriving occurs in steps:

1. Receive messages
2. Process (transform, filter, skip) messages
3. Send messages
4. Delete messages
5. Cleanup

The advanced redrive client spins up the above steps for each queue listed in the configuration.

Each step is assigned directories, which allow for manual inspection and modification during this process.

1. SQS Queue -> Receive messages -> `/received`
2. `/received` -> Process messages -> `/updates/pending`, `/skips`, or `/deletes/pending`
3. `/updates/pending` -> Send messages -> `/updates/archived` and `/deletes/pending` or `/updates/errors`
4. `/deletes/pending` -> Delete messages -> `/deletes/archived` or `/deletes/errors`
5. Optionally clean the filesystem

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

Cleanup:

`npm run redrive -- -c config/example.yaml --clean`

Or to include the `/received` directory:

`npm run redrive -- -c config/example.yaml --clean-all`

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
      /skips (place messages here to skip message)
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
