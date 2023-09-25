export namespace Constants {
  export const sqsBatchLimit = 10;

  export const deletesDirectory = "deletes";
  export const receivedDirectory = "received";
  export const skipDirectory = "skip";
  export const updatesDirectory = "updates";

  export const directoriesToCreate = [
    deletesDirectory,
    receivedDirectory,
    skipDirectory,
    updatesDirectory,
  ];

  export const pendingSubdirectory = "pending";
  export const errorsSubdirectory = "errors";
  export const archivedSubdirectory = "archived";

  export const subdirectoriesToCreate = [
    pendingSubdirectory,
    errorsSubdirectory,
    archivedSubdirectory,
  ];
}
