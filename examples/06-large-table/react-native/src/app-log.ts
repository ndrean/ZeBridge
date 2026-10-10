/// The screen's log, also kept in the app's Documents/app-log.txt: a Release build's
/// console reaches nothing the Mac can read, and a file is fetched with
///   xcrun devicectl device copy from --device <udid> --domain-type appDataContainer \
///     --domain-identifier dev.zebridge.largetable --source Documents/app-log.txt --destination .
/// The last 2,000 lines, rewritten at most every half second (no append in this
/// expo-file-system), so logging never paces the seed.
import * as FileSystem from 'expo-file-system/legacy';

const FILE = `${FileSystem.documentDirectory}app-log.txt`;
const MAX = 2000;
const lines: string[] = [];
let timer: ReturnType<typeof setTimeout> | null = null;
/// The previous launch's log is kept as app-log.prev.txt: a launch after a jetsam kill
/// used to overwrite the one file that said what the killed process was doing.
const moved = FileSystem.deleteAsync(`${FileSystem.documentDirectory}app-log.prev.txt`, { idempotent: true })
  .then(() => FileSystem.moveAsync({ from: FILE, to: `${FileSystem.documentDirectory}app-log.prev.txt` }))
  .catch(() => { /* no previous log */ });

export function fileLog(engine: string, text: string, err = false) {
  lines.push(`${new Date().toISOString()} ${engine}${err ? ' ERROR' : ''} ${text}`);
  if (lines.length > MAX) lines.splice(0, lines.length - MAX);
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    void moved.then(() => FileSystem.writeAsStringAsync(FILE, lines.join('\n') + '\n')).catch(() => { /* the next line retries */ });
  }, 500);
}
