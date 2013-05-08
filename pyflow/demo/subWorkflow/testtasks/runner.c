#include "math.h"
#include "assert.h"

int main(int argc, char**argv) {
assert(argc==2);
int mult=atoi(argv[1]);
int i,j;
double a=0;
long total=50000000;
for(j=0;j<mult;++j) {
for(i=0;i<total;++i) {
  a+=i;a=sqrt(a);
}
}
return 0;
}
