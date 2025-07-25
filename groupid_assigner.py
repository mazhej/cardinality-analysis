from collections import defaultdict
from typing import Dict, List, Tuple


class GroupIdAssigner:
    """
    Assigns GroupIDs to TokenIDs based on SmashID family logic within a P20ID.
    GroupIDs are unique ascending integers assigned to SmashID families, where a family
    includes a primary SmashID (longest length, lowest sort order in ties) and its equal or subset SmashIDs.


    Attributes:
        group_id_counter (int): Tracks the next available GroupID, incrementing by 2.
    """
    def __init__(self):
        """Initialize the GroupID counter starting at 1."""
        self.group_id_counter = 1


    def assign_group_ids(self, token_smashids: List[Tuple[int, str]]) -> Dict[int, int]:
        """
        Assign GroupIDs to TokenIDs based on SmashID subset and equality logic.


        The process:
        1. Select the longest unassigned SmashID (tie broken by lexical order).
        2. Assign it a GroupID.
        3. Assign the same GroupID to SmashIDs of equal length that match it exactly.
        4. Assign the same GroupID to shorter SmashIDs that are subsets.
        5. Repeat until all SmashIDs are assigned.


        Args:
            token_smashids (List[Tuple[int, str]]): List of (TokenID, SmashID) pairs for a P20ID.


        Returns:
            Dict[int, int]: Mapping of TokenID to its assigned GroupID.}
        """
        # Map SmashIDs to their TokenIDs
        smashid_to_tokenids = defaultdict(list)
        for token_id, smash in token_smashids:
            smashid_to_tokenids[smash].append(token_id)


        result: Dict[int, int] = {}
        unassigned_smashids = set(smashid_to_tokenids.keys())


        while unassigned_smashids:
            # Step 1: Find the longest SmashID, break ties by lexical order
            longest = sorted(unassigned_smashids, key=lambda s: (-len(s), s))[0]
            current_group_id = self.group_id_counter
            self.group_id_counter += 2  # Increment by 2


            # Step 2: Assign GroupID to the primary SmashID's TokenIDs
            for token_id in smashid_to_tokenids[longest]:
                result[token_id] = current_group_id
            unassigned_smashids.remove(longest)


            # Organize remaining SmashIDs by length for systematic comparison
            length_to_smashids = defaultdict(set)
            for smash in unassigned_smashids:
                length_to_smashids[len(smash)].add(smash)
            lengths = sorted(length_to_smashids.keys(), reverse=True)


            # Step 3: Check SmashIDs of the same length for equality
            if len(longest) in length_to_smashids:
                same_length = length_to_smashids[len(longest)]
                for smash in same_length:
                    if smash == longest:  # Exact match
                        for token_id in smashid_to_tokenids[smash]:
                            result[token_id] = current_group_id
                        unassigned_smashids.remove(smash)
                del length_to_smashids[len(longest)]


            # Step 4: Check shorter SmashIDs for subsets
            for length in lengths:
                if length < len(longest):  # Only check shorter lengths
                    for smash in length_to_smashids[length]:
                        if set(smash).issubset(set(longest)):
                            for token_id in smashid_to_tokenids[smash]:
                                result[token_id] = current_group_id
                            unassigned_smashids.remove(smash)


        return result



