import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")
scripts_files = glob.glob("scripts/*")
etc_files = glob.glob("etc/*")

# The main call
setup(name='dtsfilereceiver',
      version ='2.0.0',
      license = "GPL",
      description = "DESDM's dts file receiver codes",
      author = "Michelle Gower",
      author_email = "mgower@illinois.edu",
      packages = ['dtsfilereceiver'],
      package_dir = {'': 'python'},
      scripts = bin_files,
      data_files=[('ups',['ups/dtsfilereceiver.table']),
                  ('scripts',scripts_files),
                  ('etc',etc_files)]
      )

