TODO:
1. implement generic concept definitions (attribute names as parameters) 
2. implement imperative concepts definitions generation using Builder pattern - as Scala framework
3. Recursive queries
4. implement UDFs and table synchronizations with a database
5. add packages support
6. implement custom annotations with inheritance support
7. Implment filter concept expressions that converts concept definition (dependencies) to an expression: 
obj IS concept: 
if obj = concept.parentConcept => true
if obj.parentConcept is fact => false
else => concept.Dependencies and obj IS concept.parentConcept
8. Implement ephemeral attributes that serve as expressions
9. Investigate join pruning
12. Split parser into lexer and parser, add error handling, positioning
13. DataSources from parquet, iceberg, avro, etc files
14. Enable attribute pruning optimization in composer
15. access to parent attributes in inheritance concept?
16. Introduce dimension definition that connects an attribute to a concept
17. Introduce query to cube as a anonymous cube that can join dimension concepts
18. Introduce Attribute definition as a list of attributes, map metrics parent concepts to parent concepts of a current concept
19. Use attributes list as expressions list (in group by section)
20. Change materialization if parent concept is Aggregation Concept, merge it into parent concepts instead of CTE
21. Reuse all attributes from specific parent concept (as * in SQL)
22. Implement attribute renaming in inheritance concepts
23. Allow parent concept referencing by alias, name or index. Introduce alias for child concept.
24. Implement pattern concepts that create a bunch of concepts with variables from the list (as in terraform)
25. Think about adding dashboard configuration as code to Sens

FIXES: 
1. infer parent concept aliases if they are not given,  if there is only one concept with such name, than the alias is the same as concept name, if more than one - add counters
2. validate number and type of arguments for standard aggregate functions
3. infer attributes expressions from group dependencies for aggregate calls 
4. check for recursive definitions of child attributes
5. implement more standard SQL functions
6. Check how Having section works in concept definition.
7. add quantifier (distinct/all) support to parser
8. preceding instead of following in window function in rel converter
9. turn empty attributes in anonymous concept definition to all attributes
10. validate concepts before adding to context, infer attributes before converting to rel or sql

COMPOSER:
1. convert subqueries in where section to joins - all, some
2. optimizer - correctly replace subquery in nested concept definitions, add support for anonymous definitions
3. find a way to avoid anonymousConceptDefinition during findSubExpression. Improve isAggregateConcept in ConceptConverter.
4. materializationName support in sqlConverter

