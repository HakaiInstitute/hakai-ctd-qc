from setuptools import setup

setup(name='hakai_qc',
      version='0.0.1',
      description='Hakai methods used to test qc config a datasets',
      url='https://github.com/tayden/hakai-profile-qaqc',
      author='Jessy Barrette',
      author_email='jessy.barrette@hakai.org',
      license='MIT',
      packages=['hakai_qc'],
      include_package_data=True,
      install_requires=[
          'future',
          'requests',
          'ioos_qc',
          'gsw',
          'pyproj',
          'pandas', 'numpy',
          'hakai_api',
          'matplotlib', 'seaborn',
          'plotly', 'folium', 'ipywidgets'
      ],
      zip_safe=False)