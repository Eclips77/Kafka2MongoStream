from sklearn.datasets import fetch_20newsgroups
# import logging

interesting_categories = [
	'alt.atheism',
	'comp.graphics',
	'comp.os.ms-windows.misc',
	'comp.sys.ibm.pc.hardware',
	'comp.sys.mac.hardware',
	'comp.windows.x',
	'misc.forsale',
	'rec.autos',
	'rec.motorcycles',
	'rec.sport.baseball'
]
not_interesting_categories = [
	'rec.sport.hockey',
	'sci.crypt',
	'sci.electronics',
	'sci.med',
	'sci.space',
	'soc.religion.christian',
	'talk.politics.guns',
	'talk.politics.mideast',
	'talk.politics.misc',
	'talk.religion.misc'
]
newsgroups_interesting = fetch_20newsgroups(subset='all', categories=interesting_categories)
newsgroups_not_interesting = fetch_20newsgroups(subset='all', categories=not_interesting_categories)
