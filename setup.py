from setuptools import setup

setup(name='hakai_qc',
      version='0.0.1',
      description='Hakai methods used to config datasets',
      url='https://github.com/tayden/hakai-profile-qaqc',
      author='Jessy Barrette',
      author_email='jessy.barrette@hakai.org',
      license='MIT',
      packages=['hakai_qc'],
      install_requires=[
          'future',
          'requests',
          'ioos_qc',
          'gsw',
          'pyproj',
          'pandas',
      ],
      zip_safe=False)