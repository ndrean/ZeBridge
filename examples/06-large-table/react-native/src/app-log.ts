/// The screen's log, also kept in the app's Documents/app-log.txt: a Release build's
/// console reaches nothing the Mac can read, and a file is fetched with
///   xcrun devicectl device copy from --device <udid> --domain-type appDataContainer \
///     --domain-identifier dev.zebridge.largetable --source Documents/app-log.txt --destination .
/// The last 2,000 lines, rewritten at most every half second (no append in this
/// expo-file-system), so logging never paces the seed.
import * as FileSystem from 'expo-file-system';

const FILE = `${FileSystem.documentDirectory}app-log.txt`;
const MAX = 2000;
const lines: string[] = [];
let timer: ReturnType<typeof setTimeout> | null = null;

export function fileLog(engine: string, text: string, err = false) {
  lines.push(`${new Date().toISOString()} ${engine}${err ? ' ERROR' : ''} ${text}`);
  if (lines.length > MAX) lines.splice(0, lines.length - MAX);
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    FileSystem.writeAsStringAsync(FILE, lines.join('\n') + '\n').catch(() => { /* the next line retries */ });
  }, 500);
}
