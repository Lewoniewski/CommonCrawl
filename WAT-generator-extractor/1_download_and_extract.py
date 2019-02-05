import gzip, re, os, os.path, time, requests
if not os.path.exists("download"): os.makedirs("download")
if not os.path.exists("start"): os.makedirs("start")
if not os.path.exists("generator"): os.makedirs("generator")


with open("lista.txt","r") as oo:
    lista=oo.read().splitlines()

for li in lista:
    
    name=li.replace(".warc.wat.gz","").split("/")[-1]
    if os.path.isfile("start/"+name): continue
    print name

    with open("start/"+name,"w") as oo:
        oo.write("start")

    if not os.path.exists("download/"+name+".warc.wat.gz"):
        link = "https://commoncrawl.s3.amazonaws.com/"+li
        print link
        uaua=0
        while uaua==0:
            with open("download/"+name+".warc.wat.gz", "wb") as f:
                
                
                ddee=0
                while ddee==0:
                    
                    try:
                        response = requests.get(link, stream=True)
                        total_length = response.headers.get('content-length')
                        ddee=1
                    except:
                        print ("!!!pause600")
                        time.sleep(600)
                        

                print ("Downloading %s" % name)
                if total_length is None: # no content length header
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    try: # "Connection broken: error(104, 'Connection reset by peer')
                        for data in response.iter_content(chunk_size=4096):
                            dl += len(data)
                            f.write(data)
                        uaua=1
                    except:
                        print "!!Connection broken. Pause 600 sek"
                        try: os.remove("download/"+name+".warc.wat.gz")
                        except: print "!!! not deleted - download/"+name+".warc.wat.gz"
                        time.sleep(600)
            

                                    
    domains=set()
    domains_generators={}
    domains_generators_url={}
    nacz=0
    c=0
    
    if not os.path.isfile("download/"+name+".warc.wat.gz"): continue
    
    with gzip.open("download/"+name+".warc.wat.gz","r") as oo:
        for line in oo:
            c+=1
            if c%100000==0:
                print str(c)+"  "+name
            if "WARC-Target-URI: http" in line:
                nacz=1
                url=line.replace("WARC-Target-URI: ","")
                try: domain=url.replace("//","\t").replace("/","\t").split("\t")[1]
                except: domain="none"
            if nacz==1:
                
                if '"generator"' in line or '"Generator"' in line:
                    zz=re.findall('"name":"generator","content":"([^"]+)',line.replace("Generator","generator").replace("Content","content"))
                    for z in zz:
                        if domain not in domains_generators:
                            domains_generators[domain]=set()
                        domains_generators[domain].add(z)
                        domains_generators_url[domain+"-"+z]=url
                           
                            
                if "WARC/1.0" in line:
                    nacz=0
                    

    f=open("generator/"+name,"w")
    for key,value in domains_generators.items():
        for v in value:
            f.write(key+"\t"+v.replace("\t","").replace("\n","").replace("\r","")+"\t"+domains_generators_url[key+"-"+v].replace("\t","").replace("\n","").replace("\r","")+"\n")


            
    try: os.remove("download/"+name+".warc.wat.gz")
    except: print "!!! not deleted - download/"+name+".warc.wat.gz"
