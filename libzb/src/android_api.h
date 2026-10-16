/* NDK 30's target check needs the configured API after the compiler builtins. */
#if defined(ZB_ANDROID_API)
#undef __ANDROID_API__
#define __ANDROID_API__ ZB_ANDROID_API
#endif
