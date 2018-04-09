import os

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True


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
    version='0.1.0',
    description='Fast Python bindings for KyotoTycoon.',
    author='Charles Leifer',
    author_email='',
    packages=['kt'],
    ext_modules=cythonize([kt]),
)
