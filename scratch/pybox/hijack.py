

class A :
  def __init__(self) :
    self.x = 1

  def inc(self) :
    self.x += 1


a = A()
b = A()

a.inc()
b.inc()


# hijack:
b.inc = a.inc

b.inc()

print "a", a.x
print "b", b.x

