"""A wheel per platform: zebridge/lib/ carries that platform's libzb, so the wheel is
tagged py3-none-<platform> (any Python 3, no Python ABI: the package talks to libzb
through ctypes). scripts/build-wheels.sh sets ZB_WHEEL_PLATFORM for each one; without it,
the wheel stays pure (a source checkout uses ZB_LIB, the repository's build, or the
system's)."""
import os

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel


class PlatformWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        if os.environ.get("ZB_WHEEL_PLATFORM"):
            self.root_is_pure = False

    def get_tag(self):
        platform = os.environ.get("ZB_WHEEL_PLATFORM")
        return ("py3", "none", platform) if platform else super().get_tag()


setup(cmdclass={"bdist_wheel": PlatformWheel})
