#
# This file is a part of the normalize python library
#
# normalize is free software: you can redistribute it and/or modify
# it under the terms of the MIT License.
#
# normalize is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# MIT License for more details.
#
# You should have received a copy of the MIT license along with
# normalize.  If not, refer to the upstream repository at
# http://github.com/hearsaycorp/normalize
#

from __future__ import absolute_import

import types

from normalize.coll import Collection
import normalize.exc as exc
from normalize.record import Record
from normalize.selector import FieldSelector


class Visitor(object):
    """The Visitor object represents a single recursive visit in progress.  You
    hopefully shouldn't have to sub-class this class for most use cases; just
    VisitorPattern.
    """
    def __init__(self, apply_func, reduce_func,
                 reduce_coll_func, reduce_complex_func,
                 apply_empty_slots=False,
                 apply_records=False,
                 extraneous=False,
                 ignore_empty_string=False,
                 ignore_none=True,
                 visit_filter=None):
        """Create a new Visitor object.  Generally called by a front-end class
        method of :py:class:`VisitorPattern`

        There are four positional arguments, which specify the particular
        functions to be used during the visit.  The important options from a
        user of a visitor are the keyword arguments:

            ``apply_records=``\ *bool*
                Normally, traversal happens in depth-first order, and fields
                which are Records never have ``apply`` called on them.  If you
                want them to, set this field.  This affects the arguments
                passed to ``reduce_record``.

                If the ``apply`` function returns ``self.StopVisiting`` (or an
                instance of it), then traversal does not descend into the
                fields of the record.  If it returns something else, then
                ``reduce_record`` is passed a tuple as its first argument, and
                should act accordingly.

            ``extraneous=``\ *bool*
                Also call the apply method on properties marked *extraneous*.
                False by default.

            ``ignore_empty_string=``\ *bool*
                If the 'apply' function returns the empty string, treat it as
                if the slot or object did not exist.  ``False`` by default.

            ``ignore_none=``\ *bool*
                If the 'apply' function returns ``None``, treat it as if the
                slot or object did not exist.  ``True`` by default.

            ``visit_filter=``\ *MultiFieldSelector*
                This supplies an instance of
                :py:class:`normalize.selector.MultiFieldSelector`, and
                restricts the operation to the matched object fields.
        """
        self.apply = apply_func
        self.reduce_record = reduce_func
        self.reduce_collection = reduce_coll_func
        self.reduce_complex = reduce_complex_func
        self.apply_empty_slots = apply_empty_slots
        self.apply_records = apply_records
        self.ignore_empty_string = ignore_empty_string
        self.ignore_none = ignore_none
        self.extraneous = extraneous
        self.visit_filter = visit_filter
        self.seen = set()
        self.cue = list()

    def is_filtered(self, prop):
        return (not self.extraneous and prop.extraneous) or (
            self.visit_filter and self.cue + [prop.name] not in
            self.visit_filter
        )

    def field_selector(self):
        return FieldSelector(self.cue)

    def push(self, what):
        self.cue.append(what)

    def pop(self, what=None):
        if what is not None:
            assert(self.cue[-1] == what)
        return self.cue.pop()

    def copy(self):
        doppel = type(self)(
            self.apply, self.reduce, self.reduce_collection,
            self.reduce_complex,
            apply_empty_slots=self.apply_empty_slots,
            apply_records=self.apply_records,
            extraneous=self.extraneous,
            ignore_empty_string=self.ignore_empty_string,
            ignore_none=self.ignore_none,
            visit_filter=self.visit_filter,
        )
        for x in self.cue:
            doppel.push(x)
        return doppel


class VisitorPattern(object):
    """Base Class for writing Record visitor pattern classes.

    These classes are not instantiated, and consist purely of class methods.
    """
    Visitor = Visitor

    @classmethod
    def visit(cls, value, value_type=None, visitor=None, **kwargs):
        """The default entry point, which visits a data structure, applys a
        method ('apply') to every attribute slot, and returns the reduced
        result.  Like :py:func:`normalize.diff.diff`, this function accepts a
        series of keyword arguments, which are passed through to
        :py:class:`normalize.visitor.Visitor`.

        This function also takes positional arguments:

            ``value=``\ *object*
                The value to visit.  Normally (but not always) a
                :py:class:`normalize.record.Record` instance.

            ``value_type=``\ *RecordType*
                This is the ``Record`` subclass to interpret ``value`` as.  The
                default is ``type(value)``.  If you specify this, then the type
                information on ``value`` is essentially ignored (with the
                caveat mentioned below on :py:meth:`Visitor.map_prop`, and may
                be a ``dict``, ``list``, etc.

            ``visitor=``\ *Visitor*
                Specifies the visitor objects, which contains the options which
                customizes the descent and reduction for a particular call.
                You would not normally pass this, but instead specify keyword
                arguments as described in the
                :py:class:`normalize.visitor.Visitor` constructor.

            ``**kwargs``
                Other arguments accepted by
                :py:class:`normalize.visitor.Visitor`.

        """
        #import ipdb; ipdb.set_trace()
        if visitor is None:
            visitor = cls.Visitor(
                cls.apply, cls.reduce_record,
                cls.reduce_collection, cls.reduce_complex,
                **kwargs)

        if not value_type:
            value_type = type(value)

        return cls.map(visitor, value, value_type)

    @classmethod
    def cast(cls, value_type, value, visitor=None, **kwargs):
        """This alternate entry point is designed for operations which return
        an instance of the passed type.  The difference between this function
        and :py:meth:`normalize.visitor.VisitorPattern.visit` is that this one
        calls different methods; instead of ``apply`` and ``reduce_``\ *X*, it
        calls ``reverse`` and ``produce_``\ *X*.

        This function also takes positional arguments:

            ``value_type=``\ *RecordType*

            ``value=``\ *object*

            ``visitor=``\ *Visitor.Options*
                Specifies the visitor options, which customizes the descent
                and reduction.
        """
        if visitor is None:
            visitor = cls.Visitor(
                cls.reverse,
                cls.produce_record,
                cls.produce_collection,
                cls.produce_complex,
                **kwargs)

        return cls.map(visitor, value, value_type)

    @classmethod
    def apply(cls, value, prop, parent_obj, visitor):
        """'apply' is a general place to put a function which is called on
        every extant record slot.  This is usually the most important function
        to implement when sub-classing.

        Data structures are normally applied in depth-first order.  If you
        specify the ``apply_records`` visitor option, this function is called
        on the actual records first, and may prevent recursion by returning
        ``self.StopVisiting(return_value)``.  Any value (other than ``None``)
        returned by applying to a record changes the value passed to
        ``reduce_record``.

        The default implementation passes through the slot value as-is, but
        expected exceptions are converted to ``None``.

        args:

            ``value=``\ *value*\ \|\ *AttributeError*\ \|\ *KeyError*
                This is the value currently in the slot, or the Record itself
                with the ``apply_records`` visitor option.  *AttributeError*
                will only be received if you passed ``apply_empty_slots``, and
                *KeyError* will be passed if ``parent_obj`` is a ``dict`` (see
                :py:meth:`Visitor.map_prop` for details about when this might
                happen)

            ``prop=``\ *Property*\ \|\ ``None``
                This is the :py:class:`normalize.Property` instance which
                represents the field being traversed.  ``None`` with
                ``apply_records``

            ``parent_obj=``\ *Record*\ \|\ ``None``
                This is the instance which the value exists in.
                ``prop.get(parent_obj)`` will return ``value`` (or throw
                ``AttributeError``). ``None`` with ``apply_records``

            ``visitor=``\ *Visitor*
                This object can be used to inspect parameters of the current
                run, such as options which control which kinds of values are
                visited, which fields are being visited and where the function
                is in relation to the starting point.
        """
        return (
            None if isinstance(value, (AttributeError, KeyError)) else
            value
        )

    @classmethod
    def reduce_record(cls, applied, record_type, visitor):
        """Hook called for each record, with the results of mapping each
        member.

        The default implementation returns the first argument as-is.

        args:

            ``applied=``\ *dict*\ \|\ *tuple*
                This is the result of mapping the individual slots in the
                record as a dict.  The keys are the attribute names, and the
                values the result from their ``apply`` call.  With
                ``ignore_none`` (the default), this dictionary will be missing
                those keys.  Similarly with ``ignore_empty_string`` and empty
                string applied results.

                With ``apply_records``, if the first call to ``apply`` on the
                Record returned anything other than ``this.StopVisiting(x)`` or
                ``None``, the value will be a tuple, with the first item the
                result of the first call to ``apply`` on the Record, and the
                second item the result of applying the individual slots.  If
                you did return ``this.StopVisiting(x)``, then ``applied`` will
                be ``x``, whatever that was.

            ``record_type=``\ *RecordType*
                This is the :py:class:`normalize.Record` *class* which
                is currently being reduced.

            ``visitor=``\ *Visitor*
                ...
        """
        return applied

    @classmethod
    def reduce_collection(self, result_coll_generator, coll_type, visitor):
        """Hook called for each normalize.coll.Collection.

        The default implementation calls
        :py:meth:`normalize.coll.Collection.tuples_to_coll` with
        ``coerce=False``, which just re-assembles the collection into a native
        python collection type of the same type of the input collection.

        args:

            ``result_coll_generator=`` *generator func*
                Generator which returns (key, value) pairs (like
                :py:meth:`normalize.coll.Collection.itertuples`)

            ``coll_type=``\ *CollectionType*
                This is the :py:class:`normalize.coll.Collection`-derived
                *class* which is currently being reduced.

            ``visitor=``\ *Visitor*
                ...
        """
        return coll_type.tuples_to_coll(result_coll_generator, coerce=False)

    @classmethod
    def reduce_complex(self, record_result, coll_result, value_type, visitor):
        """If a Collection also has properties that map to something (which you
        can only do by sub-classing ``RecordList`` or another
        :py:class:`normalize.coll.Collection` and adding properties), this
        reduction is called to combine the two applied/reduced values into a
        single value for return.

        The default implementation throws ``coll_result`` into
        ``record_result['values']``, and throws an exception if it was already
        present.

        args:

            ``record_result=``\ *dict*
                This contains whatever ``reduce_record`` returned, which will
                generally be a dictionary.

            ``coll_result=``\ *list\*\ \|\ *set*\ \|\ *etc*
                This contains whatever ``reduce_collection`` returned, normally
                a list.

            ``value_type=``\ *CollectionType*
                This is the :py:class:`normalize.coll.Collection`-derived
                *class* which is currently being reduced.  Remember that
                ``Collection`` is a ``Record`` sub-class, so it has
                ``properties`` and all those other fields available.

            ``visitor=``\ *Visitor*
                ...
        """
        if record_result.get("values", False):
            raise exc.VisitorTooSimple(
                fs=visitor.field_selector(),
                value_type_name=value_type.__name__,
                visitor=type(self).__name__,
            )
        record_result['values'] = coll_result
        return record_result

    @classmethod
    def reverse(cls, value, prop, parent_obj, visitor):
        """This function is just like
        :py:meth:`normalize.visitor.VisitorPattern.apply`, except it is called
        by ``cast``, not ``visit``.  Typically, this function would reverse the
        subset of the out-bound transforms performed by ``apply`` necessary to
        satisfy the constructors.

        You only need to implement this method if you are building a reversable
        transform function.
        """
        return (
            None if isinstance(value, (AttributeError, KeyError)) else
            value
        )

    @classmethod
    def produce_record(cls, applied, record_type, visitor):
        """This is called during a ``cast`` operation in the same situations
        :py:meth:`normalize.visitor.VisitorPattern.reduce_record`` would be
        called if ``visit`` was being run.  Its main difference is that it
        calls the constructor of the type being applied instead of returning a
        dictionary.
        """
        return record_type(**applied)

    @classmethod
    def produce_collection(cls, result_coll_generator, coll_type, visitor):
        """This is called during a ``cast`` operation in the same situations
        :py:meth:`normalize.visitor.VisitorPattern.reduce_record`` would be
        called if ``visit`` was being run.  It produces a typed collection
        instead of a plain one.
        """
        return coll_type.tuples_to_coll(result_coll_generator)

    @classmethod
    def produce_complex(self, record_result, coll_result, value_type, visitor):
        """This is called during a ``cast`` operation in the same situations
        :py:meth:`normalize.visitor.VisitorPattern.reduce_complex`` would be
        called if ``visit`` was being run.
        """
        # FIXME: not great, this :-)
        record_result.values = coll_result.values
        return record_result

    @classmethod
    def map(cls, visitor, value, value_type):
        prune = False

        if issubclass(value_type, Record):
            record_mapped = cls.map_record(visitor, value, value_type)

            if record_mapped == cls.StopVisiting or isinstance(
                record_mapped, cls.StopVisiting
            ):
                record_mapped = record_mapped.return_value
                prune = True

        if not prune and issubclass(value_type, Collection):
            coll_mapped = visitor.reduce_collection(
                cls.map_collection(visitor, value, value_type),
                value_type, visitor,
            )

            if coll_mapped and record_mapped:
                return visitor.reduce_complex(
                    record_mapped, coll_mapped, value_type, visitor,
                )
            elif coll_mapped:
                return coll_mapped

        return record_mapped

    class StopVisiting(object):
        return_value = None

        def __init__(self, return_value):
            self.return_value = return_value

    @classmethod
    def map_record(self, visitor, record, record_type):
        """Method responsible for calling apply on each of the fields of the
        object, and returning the value passed to
        :py:meth:`normalize.visitor.VisitorPattern.reduce_record`.

        The default implementation first calls :py:meth:`Visitor.apply` on the
        record (with ``apply_records``), skipping recursion if that method
        returns ``self.StopVisiting`` (or an instance thereof)

        It then iterates over the properties (of the type, not the instance) in
        more-or-less random order, calling into :py:meth:`Visitor.map_prop` for
        each, the results of which end up being the dictionary return value.

        args:

            ``visitor=``\ *Visitor.Options*
                The current visitor for the visitor operation.

            ``record=``\ *Record*
                The instance being iterated over

            ``record_type=``\ *RecordType*
                The :py:class:`normalize.record.Record` sub-class which applies
                to the instance.
        """
        if visitor.apply_records:
            result = visitor.apply(record, None, None, visitor)
            if result == self.StopVisiting or \
                    isinstance(result, self.StopVisiting):
                return result.return_value

        result_dict = dict()

        for name, prop in record_type.properties.iteritems():
            mapped = self.map_prop(visitor, record, prop)
            if mapped is None and visitor.ignore_none:
                pass
            elif mapped == "" and visitor.ignore_empty_string:
                pass
            else:
                result_dict[name] = mapped

        to_reduce = (
            result_dict if not visitor.apply_records or result is None else
            (result, result_dict)
        )

        return visitor.reduce_record(to_reduce, record_type, visitor)

    @classmethod
    def map_prop(self, visitor, record, prop):
        """Method responsible for retrieving a value from the slot of an
        object, and calling :py:meth:`Visitor.apply` on it.  With
        ``apply_empty_slots``, this value will be an ``AttributeError``.

        The default implementation of this will happily handle non-Record
        objects which respond to the same accessor fetches.  Otherwise, if the
        ``record`` is a dictionary, it will pull out the named key and call
        ``Visitor.apply()`` on that instead.  With ``apply_empty_slots``, the
        value will be a ``KeyError``.

        This function will recurse back into :py:meth:`Visitor.map` if the type
        on the *property* indicates that the value contains a record.
        **caveat**: if no type hint is passed, then the type of the *value*
        will be used to determine whether or not to recurse.  This switching
        from visiting types back to visiting values will not occur if the
        structure being walked does not have Record objects, and nor will it
        occur if you specify the types of the columns being visited.

        args:
            ``visitor=``\ *Visitor*
                The visitor instance.

            ``record=``\ *Record*
                The instance being iterated over

            ``record_type=``\ *RecordType*
                The :py:class:`normalize.record.Record` sub-class which applies
                to the instance.
        """
        try:
            value = prop.__get__(record)
        except AttributeError, e:
            if isinstance(record, Record):
                value = e
            else:
                try:
                    value = record[prop.name]
                except TypeError:
                    value = e
                except KeyError, ae:
                    value = ae

        mapped = None
        if visitor.apply_empty_slots or not isinstance(
            value, (KeyError, AttributeError, types.NoneType)
        ):
            visitor.push(prop.name)

            # XXX - this fallback here is type-unsafe, and exists only for
            # those who don't declare their isa= for complex object types.
            value_type = prop.valuetype or type(value)

            if isinstance(value_type, tuple):
                mapped = self.map_type_union(
                    visitor, value, value_type, prop, record,
                )
            elif issubclass(value_type, Record):
                mapped = self.map(visitor, value, value_type)
            else:
                mapped = visitor.apply(value, prop, record, visitor)
            visitor.pop(prop.name)

        return mapped

    @classmethod
    def map_collection(self, visitor, coll, coll_type):
        """Generator method responsible for iterating over items in a
        collection, and recursively calling :py:meth:`Visitor.map()` on them to
        build a result.  Yields the tuple protocol: always (K, V).

        The default implementation calls the ``.itertuples()`` method of the
        value, falling back to ``coll_type.coll_to_tuples(coll)`` (see
        :py:meth:`normalize.coll.Collection.coll_to_tuples`).  On each item, it
        recurses to ``self.map()``.
        """
        try:
            generator = coll.itertuples()
        except AttributeError:
            generator = coll_type.coll_to_tuples(coll)

        for key, value in generator:
            visitor.push(key)  # XXX - we're in a generator.  bad.
            mapped = self.map(visitor, value, coll_type.itemtype)
            visitor.pop(key)
            if mapped is None and visitor.ignore_none:
                pass
            elif mapped == "" and visitor.ignore_empty_string:
                pass
            else:
                yield key, mapped

    @classmethod
    def map_type_union(self, visitor, value, type_tuple, prop, record):
        """This corner-case method applies when visiting a value and
        encountering a type union in the ``Property.valuetype`` field.

        It first checks to see if the value happens to be an instance of one or
        more of the types in the union, and then calls :py:meth:`Visitor.map`
        on those, in order, until one of them returns something.  If none of
        these return anything, the last one called is the applied result.

        If the value is not an instance of any of them, then it tries again,
        this time calling ``map()`` with the type of each of the ``Record``
        types in the union, in turn.  If any of them return a non-empty dict,
        then that is returned.  If none of these attempts return anything (or
        there are no ``Record`` sub-classes in the type union), then
        :py:meth:`Visitor.apply` is called on the slot instead.
        """
        # this code has the same problem that record_id does;
        # that is, it doesn't know which of the type union the
        # value is.
        record_types = []
        matching_record_types = []

        for value_type in type_tuple:
            if issubclass(value_type, Record):
                record_types.append(value_type)
            if isinstance(value, value_type):
                matching_record_types.append(value_type)

        mapped = None
        if matching_record_types:
            for value_type in matching_record_types:
                mapped = self.map(visitor, value, value_type)
                if mapped:
                    break
        else:
            for value_type in record_types:
                mapped = self.map(visitor, value, value_type)
                if mapped:
                    break

            if not mapped:
                mapped = visitor.apply(value, prop, record, visitor)

        return mapped
