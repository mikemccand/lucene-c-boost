import os
import urllib.request
import subprocess

# TODO
#  - other platforms

def run(cmd):
  print('  %s' % cmd)
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout, stderr = p.communicate()
  if p.returncode != 0:
    raise RuntimeError('command "%s" failed:\n%s' % (cmd, stdout.decode('utf-8')))

LUCENE_JAR = 'lucene-core-4.3.0.jar'

if not os.path.exists('lib'):
  os.makedirs('lib')

if not os.path.exists('lib/%s' % LUCENE_JAR):
  print('Download Apache Lucene 4.3.0')
  jar = urllib.request.urlopen('http://repo1.maven.org/maven2/org/apache/lucene/lucene-core/4.3.0/%s' % LUCENE_JAR).read()
  open('lib/%s' % LUCENE_JAR, 'wb').write(jar)

if not os.path.exists('build'):
  os.makedirs('build')

JAVA_HOME = os.environ['JAVA_HOME']

# -ftree-vectorizer-verbose=3
# -march=corei7
if not os.path.exists('dist'):
  os.makedirs('dist')
print('\nCompile NativeSearch.cpp')
run('g++ -fPIC -O4 -shared -o dist/libNativeSearch.so -I%s/include -I%s/include/linux src/c/org/apache/lucene/search/NativeSearch.cpp' % (JAVA_HOME, JAVA_HOME))
print('  done')

print('\nCompile NativeMMapDirectory.cpp')
run('g++ -fPIC -O4 -shared -o dist/libNativeMMapDirectory.so -I%s/include -I%s/include/linux src/c/org/apache/lucene/store/NativeMMapDirectory.cpp' % (JAVA_HOME, JAVA_HOME))
print('  done')

print('\nCompile java sources')
if not os.path.exists('build/classes'):
  os.makedirs('build/classes')
run('javac -XDignore.symbol.file -d build/classes -cp lib/%s src/java/org/apache/lucene/store/*.java src/java/org/apache/lucene/search/*.java' % LUCENE_JAR)
run('jar cf dist/luceneCBoost.jar -C build/classes .')
