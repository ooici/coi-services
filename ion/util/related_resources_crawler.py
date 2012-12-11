#!/usr/bin/env python

"""
@package  ion.util.related_resources_crawler
@author   Ian Katz
"""

from ooi.logging import log
from ooi import logging

class RelatedResourcesCrawler(object):


    def generate_related_resources_partial(self,
                                           resource_registry_client,
                                           predicate_list):
        """
        Generate a partial function -- the first of 3 required to find related resources

        This generator pulls the complete list of associations based on the desired predicates.

        It returns a second generator function that configures the crawl behavior

        The second generator returns the function that crawls the associaiton list given a resource id,
          returning an associaiton list
        """

        # basically a lambda function: add a list of associations-matching-a-predicate to an accumulated list
        def collect(acc, somepredicate):
            return acc + resource_registry_client.find_associations(predicate=somepredicate, id_only=False)

        # we need to "freeze" the partial function, by evaluating its internal data one time.
        def freeze():

            # get the full association list
            master_assn_list = reduce(collect, predicate_list, [])

            if log.isEnabledFor(logging.TRACE):
                summary = {}
                for a in master_assn_list:
                    label = "%s %s %s" % (a.st, a.p, a.ot)
                    if not label in summary: summary[label] = 0
                    summary[label] += 1

                log.trace("master assn list is %s", ["%s x%d" % (k, v) for k, v in summary.iteritems()])

            def get_related_resources_partial_fn(predicate_dictionary, resource_whitelist):
                """
                This function generates a resource crawler from 2 data structures representing desired crawl behavior

                The predicate dictionary is keyed on a predicate type, whose value is a 2-tuple of booleans
                  the first boolean is whether to crawl subject-object, the second boolean for object-subject
                  For example: dict([(PRED.hasModel, (False, True)]) would generate a crawler that could find
                               platforms or instruments with a given model

                The resource whitelist is a list of resource types that will be crawled.

                The return value of this function is a function that accepts a resource id and returns a list
                  of associations related (based on crawl behavior)
                """
                log.trace("get_related_resources_partial_fn predicate_dict=%s rsrc_whitelist=%s",
                          predicate_dictionary,
                          resource_whitelist)

                # assertions on data types
                assert type({}) == type(predicate_dictionary)
                for v in predicate_dictionary.values():
                    assert type((True, True)) == type(v)
                assert type([]) == type(resource_whitelist)


                def lookup_fn(resource_id):
                    """
                    return a dict of related resources as dictated by the pred dict and whitelist
                     - the key is the next resource id to crawl
                     - the value is the entire association
                    """
                    retval = {}

                    for a in master_assn_list:
                        search_sto, search_ots = predicate_dictionary[a.p]

                        if search_sto and a.s == resource_id and a.ot in resource_whitelist:
                            log.trace("lookup_fn matched %s object", a.ot)
                            retval[a.o] = a
                        elif search_ots and a.o == resource_id and a.st in resource_whitelist:
                            log.trace("lookup_fn matched %s subject", a.st)
                            retval[a.s] = a

                    return retval


                def get_related_resources_h(accum, input_resource_id, recursion_limit):
                    """
                    This is a recursive helper function that does the work of crawling for related resources

                    The accum is a tuple: (set of associations that are deemed "Related", set of "seen" resources)

                    The input resource id is the current resource being crawled

                    The recursion limit decrements with each recursive call, ending at 0.  So -1 for infinity.

                    The return value is a list of associations
                    """
                    if 0 == recursion_limit:
                        return accum

                    if -1000 > recursion_limit:
                        log.warn("Terminating related resource recursion, hit -1000")
                        return accum

                    acc, seen = accum

                    matches = lookup_fn(input_resource_id)
                    log.trace("get_related_resources_h got matches %s",
                              [dict((k, "%s %s %s" % (a.st, a.p, a.ot)) for k, a in matches.iteritems())])

                    unseen = set(matches.keys()) - seen
                    seen.add(input_resource_id)
                    acc  = acc  | set(matches.values())

                    #if log.isEnabledFor(logging.TRACE):
                    #    summary = {}
                    #    for a in acc:
                    #        label = "%s %s %s" % (a.st, a.p, a.ot)
                    #        if not label in summary: summary[label] = 0
                    #        summary[label] += 1
                    #    log.trace("acc2 is now %s", ["%s x%d" % (k, v) for k, v in summary.iteritems()])

                    def looper(acc2, input_rsrc_id):
                        return get_related_resources_h(acc2, input_rsrc_id, recursion_limit - 1)

                    h_ret = reduce(looper, unseen, (acc, seen))
                    #h_ret = reduce(looper, unseen, (acc, seen))
                    #(h_ret_acc, h_ret_seen) = h_ret
                    #log.trace("h_ret is %s", ["%s %s %s" % (a.st, a.p, a.ot) for a in h_ret_acc])
                    return h_ret


                def get_related_resources_fn(input_resource_id, recursion_limit=1024):
                    """
                    This is the function that finds related resources.

                    input_resource_id and recursion_limit are self explanatory

                    The return value is a list of associations.
                    """
                    retval, _ = get_related_resources_h((set([]), set([])), input_resource_id, recursion_limit)
                    log.trace("final_ret is %s", ["%s %s %s" % (a.st, a.p, a.ot) for a in retval])
                    return list(retval)

                return get_related_resources_fn # retval of get_related_resources_partial_fn

            return get_related_resources_partial_fn # retval of freeze()

        return freeze()


    # create a function that takes a resource_id and returns a dict of related resource_id => resource type
    def generate_get_related_resources_fn(self,
                                          resource_registry_client,
                                          resource_whitelist,
                                          predicate_dictionary):
        """
        This function is a one-liner for 2 calls that generate a "find related resources" function:
         generate_related_resources_partial(RR, pred_list)(pred_dict, resource_whitelist)

        the resource_whitelist is a simple list of allowed resource types

        the predicate_dictionary is a dict in the form predicate: (search subject-object?, search object-subject)
        """


        partial_fn = self.generate_related_resources_partial(resource_registry_client, predicate_dictionary.keys())

        return partial_fn(predicate_dictionary, resource_whitelist)

