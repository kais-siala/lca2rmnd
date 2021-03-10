from bw2data.backends.peewee.proxies import Activity, ActivityDataset as Act
from functools import reduce

class ActivitySelector():
    """
    Maps ecoinvent activites to REMIND technologies.

    Uses filter definitions from :class:`premise.InventorySet`.

    :param db: A lice cycle inventory database
    :type db: brightway2 database object
    """
    def create_expr(self, fltr={}, mask={}, filter_exact=False, mask_exact=False):
        """
        Create a :class:`peewee.Expression` from a filter dictionary.

        For the specification of the filter dictionary please refer to the
        documentation of :class:`premise.InventorySet`.

        :param fltr: string, list of strings or dictionary.
            If a string is provided, it is used to match the name field from the start (*startswith*).
            If a list is provided, all strings in the lists are used and results are joined (*or*).
            A dict can be given in the form <fieldname>: <str> to filter for <str> in <fieldname>.
            `mask`: used in the same way as `fltr`, but filters add up with each other (*and*).
            `filter_exact` and `mask_exact`: boolean, set `True` to only allow for exact matches.
        :type fltr: Union[str, lst, dict]
        :param mask: Works similar to fltr, but masks values using *and*.
        :type mask: Union[str, lst, dict]
        :param filter_exact: requires exact match when true.
        :type filter_exact: bool
        :param mask_exact: requires exact match when true.
        :type mask_exact: bool
        :return: a peewee expression to be fed to a *select*.
        :rtype: peewee.Expression

        """
        result = []

        # default field is name
        if type(fltr) == list or type(fltr) == str:
            fltr = {"name": fltr}
        if type(mask) == list or type(mask) == str:
            mask = {"name": mask}

        def sel(a, b):
            if filter_exact:
                return getattr(Act, a) == b
            else:
                return getattr(Act, a).startswith(b)

        def unsel(a, b):
            if mask_exact:
                return getattr(Act, a) != b
            else:
                return ~(getattr(Act, a).contains(b))

        assert len(fltr) > 0, "Filter dict must not be empty."

        # concat condtions
        slct = True
        for field in fltr:
            condition = fltr[field]
            if field == "reference product":
                field = "product"
            if type(condition) != list:
                exps = [sel(field, condition)]
            else:
                exps = [sel(field, c) for c in condition]
            slct = slct & reduce(lambda x, y: x | y, exps)

        for field in mask:
            condition = mask[field]
            if field == "reference product":
                field = "product"
            if type(condition) != list:
                exps = [unsel(field, condition)]
            else:
                exps = [unsel(field, c) for c in condition]
            slct = slct & reduce(lambda x, y: x & y, exps)
        return slct

    def select(self, db, expr, locs=[]):
        """
        Perform the SQL query using `expr` on `db`.

        :param db: A brightway2 database.
        :type db: brightway2.Database
        :param expr: A peewee expression.
        :type expr: peewee.Expression
        :param locs: optional, list of ecoinvent locations.
        :type locs: list
        :return: a peewee query that can be used to obtain activities
        :rtype: peewee.Query
        """
        assert type(locs) == list
        if len(locs) > 0:
            expr = expr & (Act.location.in_(locs))
        return Act.select().where(expr & (Act.database == db.name))
