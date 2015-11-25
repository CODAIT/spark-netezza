import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../"))

def run_scala_style_checks():
  os.system(os.path.join(ROOT, "dev/lint-scala"))

def main():
  run_scala_style_checks()

if __name__ == "__main__":
    main()
