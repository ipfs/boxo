package ipsl

var defaultBuiltinFrame = frame{scope: map[string]NodeCompiler{
	"all":   CompileAll,
	"empty": CompileEmpty,
}}
