from setuptools import setup, find_packages

setup(name='gwvolman',
      version='0.0.1',
      description='An example girder worker extension',
      author='Kacper Kowalik',
      author_email='xarthisius.kk@gmail.com',
      license='MIT',
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'License :: OSI Approved :: Apache Software License'
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Programming Language :: Python'
      ],
      entry_points={
          'girder_worker_plugins': [
              'gwvolman = gwvolman:GWVolumeManagerPlugin',
          ]
      },
      packages=find_packages(),
      zip_safe=False)
