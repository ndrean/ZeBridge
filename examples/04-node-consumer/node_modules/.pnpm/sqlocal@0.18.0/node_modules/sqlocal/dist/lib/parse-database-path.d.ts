type DatabasePathInfo = {
    directories: string[];
    fileName: string;
    tempFileNames: string[];
    getDirectoryHandle: () => Promise<FileSystemDirectoryHandle>;
};
export declare function parseDatabasePath(path: string): DatabasePathInfo;
export {};
