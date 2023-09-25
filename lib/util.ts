export function batch<T>(items: T[], batchSize: number): T[][] {
  return items.reduce<T[][]>(
    (batches, item) => {
      if (batches[batches.length - 1].length >= batchSize) {
        batches.push([item]);
      } else {
        batches[batches.length - 1].push(item);
      }
      return batches;
    },
    [[]],
  );
}
