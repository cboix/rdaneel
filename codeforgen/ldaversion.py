import logging, gensim, bz2, numpy, sys
import numpy as np
from gensim import corpora, models, similarities
from gensim import interfaces, utils
from gensim.corpora import IndexedCorpus
import redis
import datetime
r = redis.StrictRedis(host='localhost', port=6379, db=0)
logger = logging.getLogger('gensim.corpora.bleicorpus')
#logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)    
model = None;
corpus = None;
dict = None;

def startup():
    global model, corpus, dict, topicNumLda
    
    dict = gensim.corpora.dictionary.Dictionary.load("redditdict.txt")
    if(newCorpus):
        print("size of dictionary", len(dict) )
        # if you get a new corpus, uncomment all the stuff in the following section
        corpus = gensim.corpora.bleicorpus.BleiCorpus("redditcorpus.lda-c") # initalize corpus
        corpora.MmCorpus.serialize('corpus/reddit.mm', corpus); # store to disk, for later use
        
    corpus = corpora.MmCorpus('corpus/reddit.mm'); # load corpus
    
    
    # create model 
    if(uselda):
        numtopics = topicNumLda
        if(first):
            lda = models.LdaModel(corpus, num_topics=numtopics, id2word=dict);
            lda.save("models/ldaonly" + str(numtopics) + ".model")
            print("lda save done")
            
        model = models.ldamodel.LdaModel.load("models/ldaonly" + str(numtopics) + ".model")
    
    if(usehdp):
        print("using hdp")
        if(first):
            hdp = models.HdpModel(corpus, dict);
            #hdp.save("models/hdp.model")
            #print("hdp save done")
            #hdp = models.hdpmodel.HdpModel.load("models/hdp.model")
            # this next section converts the hdp to a lda
            startlda = hdp.hdp_to_lda();
            alpha = startlda[0];
            beta = startlda[1];
            lda = gensim.models.LdaModel(id2word=hdp.id2word,  num_topics=len(alpha), alpha=alpha, eta=hdp.m_eta) 
            lda.expElogbeta = numpy.array(beta, dtype=numpy.float32) 
            lda.save("models/hdp.model")
    
        model = models.ldamodel.LdaModel.load("models/hdp.model")

def makeMatrixOf( postitem ):
    global model, corpus, dict
    newformat = "%Y-%m-%d"
    numtopics = len( model.show_topics(-1) )
    pop = [{} for i in range(numtopics)]
    buckets = [list() for i in range(numtopics)]
    threshold = 0.5
    
    if(uselda):
        f = open("postMatrix/LDA" + str(numtopics) +"." + postitem + ".matrix", "w")
    
    if(usehdp):
        f = open("postMatrix/HDPonly." + postitem + ".matrix", "w")
        
    f2 = open("postMatrix/timeKarmaName", "w")
    
    i = 0
    while(i < len(corpus)):
        postid = r.hget("idlookup", i)
        post = r.hgetall("post:"+ postid)
        karma = post['karma']
        d = datetime.datetime.strptime(post['date'], "%c UTC" )
        time = d.strftime(newformat)
        
        for tup in model[corpus[i]]:
            if tup[1] > threshold:                
                buckets[tup[0]].append(post[postitem])
            '''
            if(time in pop[tup[0]]):
                pop[tup[0]][time] = pop[tup[0]][time] + karma * tup[1]
            else: 
                pop[tup[0]][time] = karma * tup[1]
            '''
        
        f2.write(time + " " + karma + " " + post['name'] + "\n")
        i += 1
    
    for i,topic in enumerate(buckets):
        f.write("topic:" + str(i) + "\n")
        for item in topic:
            f.write(item + "\n" )
    
    return buckets

def topicMatrix():
    global model, corpus, dict, topicNumLda
    i = 0
    l = ""
    x=""
    
    if(uselda):
        x = "LDA" + str(topicNumLda)
    if(usehdp):
        x = "HDP"
        
    f = open("postMatrix/topic" + x + "OfDocuments.matrix", 'w')

    while(i < len(corpus)):
        l += str( model[corpus[i]] ) + "\n"
        i += 1

    f.write(l)    
    


#first = True
#makeMatrix = True

def popTopics():
    global model, corpus, dict
    numtopics = len( model.show_topics(-1) )
    pop = [{} for i in range(numtopics)]
    buckets = [list() for i in range(numtopics)]
    threshold = 0.5
    
    if(uselda):
        f = open("postMatrix/popLDA" + str(numtopics) +"." + ".matrix", "w")
    
    if(usehdp):
        f = open("postMatrix/popHDP." + ".matrix", "w")
            
    i = 0
    while(i < len(corpus)):
        postid = r.hget("idlookup", i)
        post = r.hgetall("post:"+ postid)
        karma = post['karma']
        
        for tup in model[corpus[i]]:
            if tup[1] > threshold:                
                buckets[ tup[0] ].append(karma)
        i += 1
    
    for i,topic in enumerate(buckets):
        f.write("topic:" + str(i) + "\n")
        for item in topic:
            f.write(item + "\n" )
            
    return buckets    


newCorpus = False
first = False
makeMatrix = False

uselda = True
usehdp = False

topicNumLda = 20

startup()


if(makeMatrix):
    topicMatrix()
    makeMatrixOf("name")

popTopics()


'''
print(corpus[789])
postid = r.hget("idlookup", 789)
post = r.hgetall("post:"+ postid)
print(post)
print(dict[5])
'''
    
print("done")



    



