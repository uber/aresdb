class myClass{

      int* ptr2myDeviceArray;

    public:

      myClass(int size);

      ~myClass();

}



myClass::myClass(int size){

    ptr2myDeviceArray = cudaMalloc( (void**)&ptr2myDeviceArray, size);

}



myClass::~myClass(){
    cudaFree( (void**)&ptr2myDeviceArray );
}



int main(){

    myClass* ptr2myClass = new myClass();



    size_t freeMemBefore;

    size_t totalMemBefore;

    cudaMemGetInfo(&freeMemBefore, &totalMemBefore);

    std::cout << freeMemBefore << std::endl;



    delete ptr2myClass;



    size_t freeMemAfter;

    size_t totalMemAfter;

    cudaMemGetInfo(&freeMemAfter, &totalMemAfter);

    std::cout << freeMemAfter << std::endl;



    return 0;

}