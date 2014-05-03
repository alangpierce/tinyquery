import collections

import compiler
import typed_ast


class TypeContext(collections.namedtuple(
        'TypeContext', ['columns', 'aliases', 'ambig_aliases',
                        'aggregate_context'])):
    """Defines the types available at a point in code.

    This class is responsible for resolving column names into fully-qualified
    names. For example, if table1 and table2 are joined

    Fields:
        columns: An OrderedDict mapping from column name to type.
        aliases: A dict mapping any allowed aliases to their values. For
            example, the "value" column on a table "table" has full name
            "table.value" but the alias "value" also refers to it (as long as
            there are no other tables with a column named "value").
        ambig_aliases: A set of aliases that cannot be used because they are
            ambiguous. This is used for
        aggregate_context: Either None, indicating that aggregates are not
            allowed, or a TypeContext to use if we enter into an aggregate.
    """
    @classmethod
    def from_full_columns(cls, full_columns, aggregate_context):
        """Given just the columns field, fill in alias information."""
        aliases = {}
        ambig_aliases = set()
        for full_name in full_columns:
            short_name = cls.short_column_name(full_name)
            if short_name == full_name:
                continue
            if short_name in ambig_aliases:
                continue
            elif short_name in aliases:
                del aliases[short_name]
                ambig_aliases.add(short_name)
            else:
                aliases[short_name] = full_name
        return cls(full_columns, aliases, ambig_aliases, aggregate_context)

    @classmethod
    def union_contexts(cls, contexts):
        """Creates a type context from the union of others.

        This follows the semantics of the comma operator:
        -Columns are added in order, and columns already added from previous
            tables are kept in their original place.
        -All fully-qualified names are removed; columns can only be referenced
            by their direct names.
        TODO: Do better error handling with things like conflicting types.
        """
        result_columns = collections.OrderedDict()
        for context in contexts:
            assert context.aggregate_context is None

            for column_name, col_type in context.columns.iteritems():
                short_name = cls.short_column_name(column_name)
                if short_name in result_columns:
                    if result_columns[short_name] == col_type:
                        continue
                    raise compiler.CompileError(
                        'Incompatible types when performing union on field '
                        '{}: {} vs. {}'.format(
                            short_name, result_columns[short_name], col_type))
                else:
                    result_columns[short_name] = col_type
        return cls(result_columns, aliases={}, ambig_aliases=set(),
                   aggregate_context=None)

    @staticmethod
    def short_column_name(full_column_name):
        tokens = full_column_name.rsplit('.', 1)
        return tokens[-1]

    def column_ref_for_name(self, name):
        """Gets the full identifier for a column from any possible alias."""
        if name in self.columns:
            return typed_ast.ColumnRef(name, self.columns[name])
        elif name in self.aliases:
            full_name = self.aliases[name]
            return typed_ast.ColumnRef(full_name, self.columns[full_name])
        elif name in self.ambig_aliases:
            raise compiler.CompileError('Ambiguous field: {}'.format(name))
        else:
            raise compiler.CompileError('Field not found: {}'.format(name))
