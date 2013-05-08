
import smtplib
from email.MIMEText import MIMEText

def getHostName() :
    import socket
    #return socket.gethostbyaddr(socket.gethostname())[0]
    return socket.getfqdn()

def getDomainName() :
    "maybe this isn't the technical term -- this is just the hostname - the host"
    hn=getHostName().split(".")
    if len(hn)>1 : hn=hn[1:]
    return ".".join(hn)


me = "pyflow-bot@"+getDomainName()
to = "csaunders@illumina.com"

msg=MIMEText("foo foo")
msg["Subject"] = "pyFlow: job: XXX complete"
msg["From"] = me 
msg["To"] =  to

msg.as_string()

s=smtplib.SMTP('localhost')
s.sendmail(me,to,msg.as_string())
s.quit()
