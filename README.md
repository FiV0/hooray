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

### Inspiration
Some projects I have looked at and

- [asami](https://github.com/quoll/asami) - Graph database written Clojure(script)
- [xtdb](https://github.com/xtdb/xtdb) - General-purpose bitemporal database for SQL, Datalog & graph queries.

### Glossary

* triple - generally refers to an [e a v] triple, sometimes also in a different order
* pattern - refers to a non yet instantiated triple, could contain variables but does not have to
* clause - more generally a where clause. Could be a triple but maybe also a different kind of clause.

## License

I copied two files from [asami](https://github.com/quoll/asami) in the beginning which are licenced
under the EPL 1.0. The files are marked explicitly in the header.

Everything else is under the MIT Licence. In some cases copyrighted to a different party as I copied some code.
See the `LICENCE` file and the headers of files.
