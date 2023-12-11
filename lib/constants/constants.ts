export namespace Constants {
  export const sqsBatchLimit = 10;
  export const processingBatchLimit = 100;

  export const deletesDirectory = "deletes";
  export const receivedDirectory = "received";
  export const processingErrors = "processing-errors";
  export const skipsDirectory = "skips";
  export const updatesDirectory = "updates";

  export const directoryList = [
    deletesDirectory,
    receivedDirectory,
    processingErrors,
    skipsDirectory,
    updatesDirectory,
  ];

  export const pendingSubdirectory = "pending";
  export const errorsSubdirectory = "errors";
  export const archivedSubdirectory = "archived";

  export const directoriesWithSubdirectories = [
    deletesDirectory,
    updatesDirectory,
  ];

  export const subdirectoriesToCreate = [
    pendingSubdirectory,
    errorsSubdirectory,
    archivedSubdirectory,
  ];
}
