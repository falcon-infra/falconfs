#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>

thread_local static void* stack_buffer[128];
thread_local static int stack_size = 0;
static void PrintStacktrace() 
{
    fprintf(stderr, "PrintStacktrace:\n");
    if (stack_size > 0) 
    {
        char** symbols = backtrace_symbols(stack_buffer, stack_size);
        for (int i = 0; i < stack_size; i++) 
        {
            fprintf(stderr, "%s\n", symbols[i]);
        }
        free(symbols);
    }
    fflush(stderr);
}

using __cxa_throw_type = void(*)(void*, void*, void(*)(void*));
__cxa_throw_type orig_cxa_throw = nullptr;
extern "C" __attribute__((noreturn)) void __cxa_throw(void* ex, void* info, void(*dest)(void*)) 
{
    stack_size = backtrace(stack_buffer, 128);
    PrintStacktrace();
    orig_cxa_throw(ex, info, dest);
    __builtin_unreachable();
}
__attribute__((constructor)) void init_hook() 
{
    orig_cxa_throw = (__cxa_throw_type)dlsym(RTLD_NEXT, "__cxa_throw");
}