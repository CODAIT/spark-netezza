import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../"))

def run_scala_style_checks():

  os.system(os.path.join(ROOT, "dev/lint-scala"))

def run_scala_tests():

  os.system(os.path.join(ROOT, "build/sbt test"))

def main():
  run_scala_style_checks()
  run_scala_tests()

if __name__ == "__main__":
    main()
