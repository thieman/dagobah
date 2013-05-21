from setuptools import setup

setup(name='dagobah',
      version='0.1',
      description='Simple DAG-based job scheduler',
      url='http://github.com/tthieman/dagobah',
      author='Travis Thieman',
      author_email='travis.thieman@gmail.com',
      license='BSD License',
      packages=['dagobah'],
      zip_safe=False,
      install_requires=['croniter', 'pyyaml'])
