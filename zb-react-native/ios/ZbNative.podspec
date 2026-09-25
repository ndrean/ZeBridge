Pod::Spec.new do |s|
  s.name           = 'ZbNative'
  s.version        = '0.1.0'
  s.summary        = 'libzb, the ZeBridge C client, for this app'
  s.description    = 'The Expo module over libzb (scripts/build-ios.sh builds ZbCore.xcframework).'
  s.author         = ''
  s.homepage       = 'https://docs.expo.dev/modules/'
  s.platforms      = { :ios => '15.1' }
  s.swift_version  = '5.4'
  s.source         = { git: '' }
  s.static_framework = true

  s.dependency 'ExpoModulesCore'

  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
    'SWIFT_COMPILATION_MODE' => 'wholemodule'
  }

  s.source_files = '*.{h,swift}'
  s.public_header_files = 'zb.h'
  s.vendored_frameworks = 'ZbCore.xcframework'
end
