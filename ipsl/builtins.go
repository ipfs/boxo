package ipsl

var defaultBuiltinFrame = frame{scope: ScopeMapping{
	"all":   compileAll,
	"empty": compileEmpty,
}}
