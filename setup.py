from setuptools import setup

setup(name='dagobah',
      version='0.1',
      description='Simple DAG-based job scheduler',
      url='http://github.com/tthieman/dagobah',
      author='Travis Thieman',
      author_email='travis.thieman@gmail.com',
      license='BSD',
      packages=['dagobah'],
      package_data={'dagobah': ['dagobah/email/templates/*',
                                'dagobah/daemon/dagobahd.yaml',
                                'dagobah/daemon/static/*',
                                'dagobah/daemon/templates/*']},
      zip_safe=False,
      install_requires=['croniter', 'pyyaml', 'premailer'],
      test_suite='nose.collector',
      tests_require=['nose', 'pymongo'],
      entry_points={'console_scripts':
                    ['dagobahd = dagobah.daemon.app.daemon_entrypoint']
                    })
