import collections

import context
import typed_ast


class Evaluator(object):
    def __init__(self, tables_by_name):
        self.tables_by_name = tables_by_name

    def evaluate_select(self, select_ast):
        """Given a select statement, return a Context with the results."""
        assert isinstance(select_ast, typed_ast.Select)

        table_context = self.evaluate_table_expr(select_ast.table)
        mask_column = self.evaluate_expr(select_ast.where_expr, table_context)
        select_context = context.mask_context(table_context, mask_column)

        if select_ast.group_set is not None:
            result = self.evaluate_groups(
                select_ast.select_fields, select_ast.group_set, select_context)
        else:
            result = self.evaluate_select_fields(
                select_ast.select_fields, select_context)
        if select_ast.limit is not None:
            context.truncate_context(result, select_ast.limit)
        return result

    def evaluate_groups(self, select_fields, group_set, select_context):
        """Evaluate a list of select fields, grouping by some of the values.

        Arguments:
            select_fields: A list of SelectField instances to evaluate.
            group_set: The groups (either fields in select_context or aliases
                referring to an element of select_fields) to group by.
            select_context: A context with the data that the select statement
                has access to.

        Returns:
            A context with the results.
        """
        field_groups = group_set.field_groups
        alias_groups = group_set.alias_groups
        alias_group_list = sorted(alias_groups)

        group_key_select_fields = [
            f for f in select_fields if f.alias in alias_groups]
        aggregate_select_fields = [
            f for f in select_fields if f.alias not in alias_groups]

        alias_group_result_context = self.evaluate_select_fields(
            group_key_select_fields, select_context)

        # Dictionary mapping (singleton) group key context to the context of
        # values for that key.
        group_contexts = {}

        # As a special case, we check if we are grouping by nothing (in other
        # words, if the query had an aggregate without any explicit GROUP BY).
        # Normally, it's fine to just use the trivial group set: every row maps
        # to the empty tuple, so we have a single aggregation over the entire
        # table. However, if the table is empty, we still want to aggregate
        # over the empty table and return a single row, so this is the one case
        # where it's possible to have a group with no rows in it. To make this
        # case work, we ensure that the trivial group key (the empty tuple)
        # always shows up for the TRIVIAL_GROUP_SET case.
        # In the long run, it might be cleaner to view TRIVIAL_GROUP_SET as a
        # completely separate case, but this approach should work.
        if group_set == typed_ast.TRIVIAL_GROUP_SET:
            trivial_ctx = context.Context(1, collections.OrderedDict(), None)
            group_contexts[trivial_ctx] = (
                context.empty_context_from_template(select_context))

        # TODO: Seems pretty ugly and wasteful to use a whole context as a
        # group key.
        for i in xrange(select_context.num_rows):
            key = self.get_group_key(
                field_groups, alias_group_list, select_context,
                alias_group_result_context, i)
            if key not in group_contexts:
                new_group_context = context.empty_context_from_template(
                    select_context)
                group_contexts[key] = new_group_context
            group_context = group_contexts[key]
            context.append_row_to_context(src_context=select_context, index=i,
                                          dest_context=group_context)

        result_context = self.empty_context_from_select_fields(select_fields)
        result_col_names = [field.alias for field in select_fields]
        for context_key, group_context in group_contexts.iteritems():
            group_eval_context = context.Context(
                1, context_key.columns, group_context)
            group_aggregate_result_context = self.evaluate_select_fields(
                aggregate_select_fields, group_eval_context)
            full_result_row_context = self.merge_contexts_for_select_fields(
                result_col_names, group_aggregate_result_context, context_key)
            context.append_row_to_context(full_result_row_context, 0,
                                          result_context)
        return result_context

    def merge_contexts_for_select_fields(self, col_names, context1, context2):
        """Build a context that combines columns of two contexts.

        The col_names argument is a list of strings that specifies the order of
        the columns in the result. Note that not every column must be used, and
        columns in context1 take precedence over context2 (this happens in
        practice with non-alias groups that are part of the group key).
        """
        assert context1.num_rows == context2.num_rows
        assert context1.aggregate_context is None
        assert context2.aggregate_context is None
        # Select fields always have the None table.
        col_keys = [(None, col_name) for col_name in col_names]
        columns1, columns2 = context1.columns, context2.columns
        return context.Context(context1.num_rows, collections.OrderedDict(
            (col_key, columns1.get(col_key) or columns2[col_key])
            for col_key in col_keys
        ), None)

    def get_group_key(self, field_groups, alias_groups, select_context,
                      alias_group_result_context, index):
        """Computes a singleton context with the values for a group key.

        The evaluation has already been done; this method just selects the
        values out of the right contexts.

        Arguments:
            field_groups: A list of ColumnRefs for the field groups to use.
            alias_groups: A list of strings of alias groups to use.
            select_context: A context with the data for the table expression
                being selected from.
            alias_group_result_context: A context with the data for the
                grouped-by select fields.
            index: The row index to use from each context.
        """
        result_columns = collections.OrderedDict()
        for field_group in field_groups:
            column_key = (field_group.table, field_group.column)
            source_column = select_context.columns[column_key]
            result_columns[column_key] = context.Column(
                source_column.type, [source_column.values[index]])
        for alias_group in alias_groups:
            column_key = (None, alias_group)
            source_column = alias_group_result_context.columns[column_key]
            result_columns[column_key] = context.Column(
                source_column.type, [source_column.values[index]])
        return context.Context(1, result_columns, None)

    def empty_context_from_select_fields(self, select_fields):
        return context.Context(
            0,
            collections.OrderedDict(
                ((None, select_field.alias),
                 context.Column(select_field.expr.type, []))
                for select_field in select_fields
            ),
            None)

    def evaluate_select_fields(self, select_fields, ctx):
        """Evaluate a table result given the data the fields have access to.

        Arguments:
            select_fields: A list of typed_ast.SelectField values to evaluate.
            context: The "source" context that the expressions can access when
                being evaluated.
        """
        return context.Context(
            ctx.num_rows,
            collections.OrderedDict(
                self.evaluate_select_field(select_field, ctx)
                for select_field in select_fields),
            None)

    def evaluate_select_field(self, select_field, ctx):
        """Given a typed select field, return a resulting column entry."""
        assert isinstance(select_field, typed_ast.SelectField)
        results = self.evaluate_expr(select_field.expr, ctx)
        return (None, select_field.alias), context.Column(
            select_field.expr.type, results)

    def evaluate_table_expr(self, table_expr):
        """Given a table expression, return a Context with its values."""
        try:
            method = getattr(self,
                             'eval_table_' + table_expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for table type {}'.format(
                    table_expr.__class__.__name__))
        return method(table_expr)

    def eval_table_NoTable(self, table_expr):
        # If the user isn't selecting from any tables, just specify that there
        # is one column to return and no table accessible.
        return context.Context(1, collections.OrderedDict(), None)

    def eval_table_Table(self, table_expr):
        """Get the values from the table.

        The type context in the table expression determines the actual column
        names to output, since that accounts for any alias on the table.
        """
        table = self.tables_by_name[table_expr.name]
        return context.context_from_table(table, table_expr.type_ctx)

    def eval_table_TableUnion(self, table_expr):
        result_context = context.empty_context_from_type_context(
            table_expr.type_ctx)
        for table in table_expr.tables:
            table_result = self.evaluate_table_expr(table)
            context.append_partial_context_to_context(table_result,
                                                      result_context)
        return result_context

    def eval_table_Join(self, table_expr):
        result_context_1 = self.evaluate_table_expr(table_expr.table1)
        result_context_2 = self.evaluate_table_expr(table_expr.table2)

        table_1_key_refs = [cond.column1 for cond in table_expr.conditions]
        table_2_key_refs = [cond.column2 for cond in table_expr.conditions]

        # Build a map from table 2 key to value.
        table_2_key_contexts = {}
        for i in xrange(result_context_2.num_rows):
            key = self.get_join_key(result_context_2, table_2_key_refs, i)
            if key not in table_2_key_contexts:
                new_group_context = context.empty_context_from_template(
                    result_context_2)
                table_2_key_contexts[key] = new_group_context
            context.append_row_to_context(
                src_context=result_context_2, index=i,
                dest_context=table_2_key_contexts[key])

        result_context = context.cross_join_contexts(
            context.empty_context_from_template(result_context_1),
            context.empty_context_from_template(result_context_2),
        )
        for i in xrange(result_context_1.num_rows):
            key = self.get_join_key(result_context_1, table_1_key_refs, i)
            if key not in table_2_key_contexts:
                # Left outer join means that if we didn't find something, we
                # still put in a row with nulls on the right.
                if table_expr.is_left_outer:
                    row_context = context.row_context_from_context(
                        result_context_1, i)
                    context.append_context_to_context(row_context,
                                                      result_context)
                continue
            row_context = context.row_context_from_context(result_context_1, i)
            new_rows = context.cross_join_contexts(row_context,
                                                   table_2_key_contexts[key])
            context.append_context_to_context(new_rows, result_context)

        return result_context

    def get_join_key(self, table_context, key_column_refs, index):
        """Get the join key for a row in a table that is part of a join.

        Note that, while this code is similar to the code that computes group
        keys, groups are different because they need to be specifically
        selected from later (so we build a context for them), whereas join keys
        can have different column names.

        Arguments:
            table_context: A Context containing the data in one of the tables
                being joined.
            key_column_refs: A list of ColumnRef specifying the columns to use
                in the key and their order.
            index: The index of the row to compute the key for.

        Returns: A tuple of values for the key for this row.
        """
        return tuple(
            table_context.column_from_ref(col_ref).values[index]
            for col_ref in key_column_refs)

    def eval_table_Select(self, table_expr):
        """Evaluate a select table expression.

        The output matches the type context on the select rather than the
        directly-evaluated type context so that we account for any alias that
        might have been assigned.
        """
        result_context = self.evaluate_select(table_expr)
        return context.context_with_overlayed_type_context(result_context,
                                                           table_expr.type_ctx)

    def evaluate_expr(self, expr, context):
        """Computes the raw data for the output column for the expression."""
        try:
            method = getattr(self, 'evaluate_' + expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for type {}'.format(expr.__class__.__name__))
        return method(expr, context)

    def evaluate_FunctionCall(self, func_call, context):
        arg_results = [self.evaluate_expr(arg, context)
                       for arg in func_call.args]
        return func_call.func.evaluate(context.num_rows, *arg_results)

    def evaluate_AggregateFunctionCall(self, func_call, context):
        # Switch to the aggregate context when evaluating the arguments to the
        # aggregate.
        assert context.aggregate_context is not None, (
            'Aggregate function called without a valid aggregate context.')
        arg_results = [self.evaluate_expr(arg, context.aggregate_context)
                       for arg in func_call.args]
        return func_call.func.evaluate(context.num_rows, *arg_results)

    def evaluate_Literal(self, literal, context):
        return [literal.value for _ in xrange(context.num_rows)]

    def evaluate_ColumnRef(self, column_ref, ctx):
        column = ctx.columns[(column_ref.table, column_ref.column)]
        return column.values
