import os
import urllib.request
import subprocess

# TODO
#  - other platforms

def getMavenJAR(groupID, artifactID, version):
  url = 'http://search.maven.org/remotecontent?filepath=%s/%s/%s/%s-%s.jar' % (groupID.replace('.', '/'), artifactID, version, artifactID, version)
  return urllib.request.urlopen(url).read()

def run(cmd):
  print('  %s' % cmd)
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout, stderr = p.communicate()
  if p.returncode != 0:
    raise RuntimeError('command "%s" failed:\n%s' % (cmd, stdout.decode('utf-8')))
  return stdout

def toClassPath(deps):
  l = []
  for tup in deps:
    l.append('lib/%s-%s.jar' % (tup[1], tup[2]))
  return ':'.join(l)

DEPS = (
  ('org.apache.lucene', 'lucene-core', '4.3.0'),)

TEST_DEPS = (
  ('junit', 'junit', '4.10'),
  ('org.apache.lucene', 'lucene-codecs', '4.3.0'),
  ('org.apache.lucene', 'lucene-test-framework', '4.3.0'),
  ('com.carrotsearch.randomizedtesting', 'junit4-ant', '2.0.9'),
  ('com.carrotsearch.randomizedtesting', 'randomizedtesting-runner', '2.0.9'))
  
if not os.path.exists('lib'):
  os.makedirs('lib')

for groupID, artifactID, version in DEPS + TEST_DEPS:
  fileName = 'lib/%s-%s.jar' % (artifactID, version)
  if not os.path.exists(fileName):
    print('Download %s-%s.jar' % (artifactID, version))
    jar = getMavenJAR(groupID, artifactID, version)
    open(fileName, 'wb').write(jar)

if not os.path.exists('build'):
  os.makedirs('build')

JAVA_HOME = os.environ['JAVA_HOME']

if not os.path.exists('dist'):
  os.makedirs('dist')
print('\nCompile NativeSearch.cpp')
# -ftree-vectorizer-verbose=3
# -march=corei7
run('g++ -fPIC -O4 -shared -o dist/libNativeSearch.so -I%s/include -I%s/include/linux src/c/org/apache/lucene/search/NativeSearch.cpp' % (JAVA_HOME, JAVA_HOME))
print('  done')

print('\nCompile NativeMMapDirectory.cpp')
run('g++ -fPIC -O4 -shared -o dist/libNativeMMapDirectory.so -I%s/include -I%s/include/linux src/c/org/apache/lucene/store/NativeMMapDirectory.cpp' % (JAVA_HOME, JAVA_HOME))
print('  done')

print('\nCompile java sources')
if not os.path.exists('build/classes/java'):
  os.makedirs('build/classes/java')
run('javac -XDignore.symbol.file -d build/classes/java -cp %s src/java/org/apache/lucene/store/*.java src/java/org/apache/lucene/search/*.java' % toClassPath(DEPS))
run('jar cf dist/luceneCBoost.jar -C build/classes/java .')

if not os.path.exists('build/classes/test'):
  os.makedirs('build/classes/test')

run('javac -d build/classes/test -cp %s:dist/luceneCBoost.jar src/test/org/apache/lucene/search/*.java' % toClassPath(DEPS + TEST_DEPS))

print('\nRun tests')
if not os.path.exists('build/test'):
  os.makedirs('build/test')

command = 'export LD_LIBRARY_PATH=%s/dist; java -Xmx128m -ea' % os.getcwd()
command += ' -cp %s:build/classes/test:dist/luceneCBoost.jar' % toClassPath(DEPS + TEST_DEPS)
command += ' -DtempDir=build/test'
command += ' -Dtests.codec=Lucene42'
command += ' -Dtests.directory=NativeMMapDirectory'
command += ' -Dtests.seed=0'
command += ' org.junit.runner.JUnitCore'
command += ' org.apache.lucene.search.TestNativeSearch'
p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

while True:
  s = p.stdout.readline()
  if s == b'':
    break
  print(s.decode('utf-8').rstrip())
p.wait()
if p.returncode != 0:
  raise RuntimeError('test failed: %s')
