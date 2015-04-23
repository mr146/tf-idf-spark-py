#!/usr/local/bin/python3
import sys
import xml.etree.ElementTree

filename = sys.argv[1]
e = xml.etree.ElementTree.parse(filename).getroot()
texts = e.findall('text')
count = len(texts)
print('Texts count = {0}'.format(count))
counter = 0
for text in texts:
	allwords = []
	#print(xml.etree.ElementTree.tostring(text, 'utf-8'))
	tokens = text.findall(".//tfr")
	print(len(tokens))
	for token in tokens:
		nouns = token.findall("./v/l/*[@v='NOUN']/..")
		adjfs = token.findall("./v/l/*[@v='ADJF']/..")
		result = list(map(lambda x: x.get('t'), nouns + adjfs))
		if len(result) > 0:
			allwords.append(result[0])
		#break
	#break
	f = open('parsed/result{0}.txt'.format(counter), 'w')
	res = '{0}: {1}'.format(counter, " ".join(allwords))
	f.write(res)
	f.close()
	counter = counter + 1
	print('Processed {0} out of {1}', counter, count)