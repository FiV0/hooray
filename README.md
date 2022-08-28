# {project}

Executing the program can either be done via
```
clj -M -m scratch :arg1 :arg2
```
or by compiling a jar via
```
clj -T:build clean
clj -T:build jar
```
and executing it via
```
java -jar target/lib-0.1.4.jar :arg1 :arg2
```

### Glossary

* triple - generally refers to an [e a v] triple, sometimes also in a different order
* pattern - refers to a non yet instantiated triple, could contain variables but does not have to
* clause - more generally a where clause. Could be a triple but maybe also a different kind of clause.

## License
