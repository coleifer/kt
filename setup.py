import os
import warnings

from setuptools import setup
from setuptools.extension import Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True

try:
    from kt import __version__
except ImportError:
    warnings.warn('could not import kt module to determine version')
    __version__ = '0.0.0'


if cython_installed:
    python_source = 'kt/_binary.pyx'
else:
    python_source = 'kt/_binary.c'
    cythonize = lambda obj: obj

kt = Extension(
    'kt._binary',
    #extra_compile_args=['-g', '-O0'],
    #extra_link_args=['-g'],
    sources=[python_source])

setup(
    name='kt',
    version=__version__,
    description='Fast Python bindings for KyotoTycoon.',
    author='Charles Leifer',
    author_email='',
    packages=['kt'],
    ext_modules=cythonize([kt]),
)
