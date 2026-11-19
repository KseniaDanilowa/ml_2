"""Упаковать исполненные работы, не добавляя шаблоны, кеши и служебные БД."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import hashlib

root = Path(__file__).resolve().parents[1]
stack = root / 'data-stack'
work = stack / 'work/students/user1'
files = [root / 'README.md', root / 'deliverables/report_7.pdf', stack / 'INSTALL.md', stack / 'docker-compose.yml']
for pattern in ['Dockerfile.*','requirements-*.txt','dags/*.py','s3-init/*.py','postgres-init/*.sql']:
    files.extend(stack.glob(pattern))
files.extend(root.glob('tools/*.py'))
files.extend(work / n for n in ['project_report_7.ipynb','customers_7.csv','make_dataset.py',
                                '01_s3.ipynb','02_mlflow.ipynb','03_airflow.ipynb'])
files.extend(p for p in (work / 'evidence').rglob('*') if p.is_file())
zpath = root / 'deliverables/submission_7.zip'
with ZipFile(zpath, 'w', ZIP_DEFLATED, compresslevel=7) as z:
    sums = []
    for p in sorted(files):
        name = p.relative_to(root).as_posix()
        z.write(p, name)
        sums.append(hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + name)
    z.writestr('SHA256SUMS.txt', '\n'.join(sums)+'\n')
with ZipFile(zpath) as z:
    assert z.testzip() is None
    print(zpath.name, len(z.namelist()), 'файлов,', zpath.stat().st_size, 'байт')
