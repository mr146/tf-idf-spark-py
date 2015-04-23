# tf-idf-spark-py

parser.py [zzz.xml] - выплевыает содержимое zzz.xml в файлы parsed/result{0}.xml, где {0} - номер документа. Содержимое файла: "{0}: [word, [...]]", {0} - номер документа.

spark-submit counter.py [dir] [docid] [docscount] - выведет топ по документу с номером docid среди docscount документов, которые находятся в dir.
