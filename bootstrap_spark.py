import os
import sys


def prepare():
    """Java, winutils і Python 3.11. Викликати до імпорту pyspark."""
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

    if sys.version_info >= (3, 12):
        sys.exit(
            "PySpark 3.5 з цього проєкту розрахований на Python 3.11.\n"
            "Запуск: py -3.11 <скрипт>"
        )

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    def use_dir(path):
        if path and os.path.isdir(path):
            os.environ["PATH"] = path + os.pathsep + os.environ.get("PATH", "")
            return True
        return False

    tools = os.path.join(os.environ.get("USERPROFILE", ""), "tools")
    if not os.environ.get("JAVA_HOME"):
        jdk_root = os.path.join(tools, "jdk")
        if os.path.isdir(jdk_root):
            for name in sorted(os.listdir(jdk_root), reverse=True):
                home = os.path.join(jdk_root, name)
                if os.path.isdir(os.path.join(home, "bin")):
                    os.environ["JAVA_HOME"] = home
                    use_dir(os.path.join(home, "bin"))
                    break
    if not os.environ.get("HADOOP_HOME"):
        hadoop = os.path.join(tools, "hadoop")
        if os.path.isdir(os.path.join(hadoop, "bin")):
            os.environ["HADOOP_HOME"] = hadoop
            use_dir(os.path.join(hadoop, "bin"))
    if os.environ.get("HADOOP_HOME"):
        os.environ["hadoop.home.dir"] = os.environ["HADOOP_HOME"]


def session(app_name, jar_path=None):
    prepare()
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    )
    if jar_path:
        # spark.jars ріже аргумент по пробілу, тож конектор копіюється в теку без пробілів.
        import shutil
        from pathlib import Path

        source = Path(jar_path)
        safe = source
        if " " in str(source):
            tools = Path(os.environ.get("USERPROFILE", ".")) / "tools"
            tools.mkdir(parents=True, exist_ok=True)
            safe = tools / source.name
            if not safe.exists() or safe.stat().st_size != source.stat().st_size:
                shutil.copy2(source, safe)
        builder = builder.config("spark.jars", safe.resolve().as_uri())
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
