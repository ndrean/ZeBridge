export function parseDatabasePath(path) {
    const directories = path.split(/[\\/]/).filter((part) => part !== '');
    const fileName = directories.pop();
    if (!fileName) {
        throw new Error('Database path is invalid.');
    }
    const tempFileNames = ['journal', 'wal', 'shm'].map((ext) => `${fileName}-${ext}`);
    const getDirectoryHandle = async () => {
        let dirHandle = await navigator.storage.getDirectory();
        for (let dirName of directories)
            dirHandle = await dirHandle.getDirectoryHandle(dirName);
        return dirHandle;
    };
    return {
        directories,
        fileName,
        tempFileNames,
        getDirectoryHandle,
    };
}
//# sourceMappingURL=parse-database-path.js.map