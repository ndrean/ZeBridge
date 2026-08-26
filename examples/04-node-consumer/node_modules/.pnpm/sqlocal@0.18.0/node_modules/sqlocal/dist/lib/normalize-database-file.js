export async function normalizeDatabaseFile(dbFile, convertStreamTo) {
    let bufferOrStream;
    if (dbFile instanceof Blob) {
        bufferOrStream = dbFile.stream();
    }
    else {
        bufferOrStream = dbFile;
    }
    if (bufferOrStream instanceof ReadableStream && convertStreamTo) {
        const stream = bufferOrStream;
        const reader = stream.getReader();
        switch (convertStreamTo) {
            case 'callback':
                return async () => {
                    const chunk = await reader.read();
                    if (chunk.done)
                        reader.releaseLock();
                    return chunk.value;
                };
            case 'buffer':
                const chunks = [];
                let streamDone = false;
                while (!streamDone) {
                    const chunk = await reader.read();
                    if (chunk.value)
                        chunks.push(chunk.value);
                    streamDone = chunk.done;
                }
                reader.releaseLock();
                const arrayLength = chunks.reduce((length, chunk) => {
                    return length + chunk.length;
                }, 0);
                const buffer = new Uint8Array(arrayLength);
                let offset = 0;
                chunks.forEach((chunk) => {
                    buffer.set(chunk, offset);
                    offset += chunk.length;
                });
                return buffer.buffer;
        }
    }
    else {
        return bufferOrStream;
    }
}
//# sourceMappingURL=normalize-database-file.js.map