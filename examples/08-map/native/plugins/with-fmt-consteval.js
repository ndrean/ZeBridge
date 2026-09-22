/// Expo config plugin: turn OFF fmt's compile-time format-string checking.
///
/// §10ii: Apple clang 21 (Xcode 26) tightened how it validates C++20 `consteval`, and
/// the `FMT_STRING(...)` pattern in fmt 11.0.2 no longer satisfies it. React Native
/// 0.76 bundles exactly that fmt through RCT-Folly, so the Pods build dies five times
/// in `fmt/format-inl.h` before any of our code is reached:
///
///   call to consteval function 'fmt::basic_format_string<...>' is not a constant expression
///
/// Turning it off moves fmt's format-string validation from compile time to run time,
/// which is where it sat before C++20 anyway. Nothing of ours calls fmt; this is React
/// Native's own logging.
///
/// ⚠️ A `-DFMT_USE_CONSTEVAL=0` does NOT work. fmt 11.0.2 defines the macro through an
/// unguarded `#if/#elif/#else` chain with no `#ifndef` around it, so the header's own
/// value always wins over anything the build settings pass. The patch therefore edits
/// the header, and it has to run AFTER CocoaPods has fetched it — which is why this
/// lives in `post_install` rather than in the plugin's own file-writing step.
///
/// ⚠️ TRANSITIONAL. The real fix is React Native >= 0.83.9 / Expo SDK 56, which bundle
/// fmt 12.1.0 and compile cleanly here. Delete this plugin at that upgrade.
///
/// It is a plugin rather than a Podfile edit because `expo prebuild` REGENERATES the
/// Podfile from app.json — a hand edit survives until the next prebuild and no longer.
const { withDangerousMod } = require('expo/config-plugins');
const fs = require('node:fs');
const path = require('node:path');

const BLOCK = `
    # BEGIN with-fmt-consteval (Expo config plugin — see plugins/with-fmt-consteval.js)
    fmt_base = File.join(installer.sandbox.root, 'fmt', 'include', 'fmt', 'base.h')
    if File.exist?(fmt_base)
      src = File.read(fmt_base)
      unless src.include?('ZEBRIDGE_FMT_CONSTEVAL')
        # The two arms of fmt's detection chain that turn consteval ON. Apple clang 21
        # rejects the FMT_STRING pattern they enable; every other arm already says 0.
        patched = src
          .sub("#elif defined(__cpp_consteval)\n#  define FMT_USE_CONSTEVAL 1",
               "#elif defined(__cpp_consteval)\n#  define FMT_USE_CONSTEVAL 0  // ZEBRIDGE_FMT_CONSTEVAL: Apple clang 21")
          .sub("#elif FMT_GCC_VERSION >= 1002 || FMT_CLANG_VERSION >= 1101\n#  define FMT_USE_CONSTEVAL 1",
               "#elif FMT_GCC_VERSION >= 1002 || FMT_CLANG_VERSION >= 1101\n#  define FMT_USE_CONSTEVAL 0  // ZEBRIDGE_FMT_CONSTEVAL")
        raise "with-fmt-consteval: fmt/base.h did not match the expected shape" if patched == src
        File.write(fmt_base, patched)
        Pod::UI.puts "[with-fmt-consteval] FMT_USE_CONSTEVAL forced to 0 in fmt/base.h"
      end
    end
    # END with-fmt-consteval
`;

module.exports = function withFmtConsteval(config) {
  return withDangerousMod(config, [
    'ios',
    async (cfg) => {
      const file = path.join(cfg.modRequest.platformProjectRoot, 'Podfile');
      const src = fs.readFileSync(file, 'utf8');
      if (src.includes('with-fmt-consteval')) return cfg;
      // ⚠️ A Podfile may have exactly ONE `post_install`; CocoaPods refuses a second
      // with "Specifying multiple `post_install` hooks is unsupported". React Native's
      // template already has one, so this goes INSIDE it rather than beside it.
      const at = src.indexOf('post_install do |installer|');
      if (at === -1) throw new Error('with-fmt-consteval: no post_install hook to extend');
      const nl = src.indexOf('\n', at) + 1;
      fs.writeFileSync(file, src.slice(0, nl) + BLOCK + src.slice(nl));
      return cfg;
    },
  ]);
};
