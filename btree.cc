#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) :
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize,
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique)
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) {
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) {
      return rc;
    }

    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) {
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) {
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;

      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock

  return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) {
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) {
	if (op==BTREE_OP_LOOKUP) {
	  return b.GetVal(offset,value);
	} else {
	  // BTREE_OP_UPDATE
	  // WRITE ME
	  rc = b.SetVal(offset, value);
    if(rc){
      return rc;
    }
    else{
      return b.Serialize(buffercache, node);
    }
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) {
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) {
      } else {
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) {
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) {
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) {
      if (offset==0) {
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) {
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) {
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) {
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) {
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) {
    os << "\" ]";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  if(key.length != superblock.info.keysize || value.length != superblock.info.valuesize){
    return ERROR_SIZE;
  }


  BTreeNode originalRoot;
  ERROR_T rc;
  SIZE_T offset;
  SIZE_T ptr;
  KEY_T testkey;
  KEY_T newKey = key;
  VALUE_T newValue = value;

  rc = originalRoot.Unserialize(buffercache, superblock.info.rootnode);
  if(rc){
    return rc;
  }


    if(Lookup(newKey, newValue)!=ERROR_NONEXISTENT){
      return ERROR_CONFLICT;
    }

  //Root is empty? Create left leaf with inserted val and right leaf for future use
  if(originalRoot.info.numkeys == 0){
    BTreeNode newleaf(BTREE_LEAF_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());



    newleaf.info.rootnode = superblock_index + 1;
    newleaf.info.numkeys = 0;
    SIZE_T leftNode; //the important one
    SIZE_T rightNode;
    rc = AllocateNode(leftNode);
    if(rc){
      return rc;
    }
    rc = AllocateNode(rightNode);
    if(rc){
      return rc;
    }
    //empty right leaf
    rc = newleaf.Serialize(buffercache, rightNode);
    if (rc) {return rc;}
    //now populate left leaf
    newleaf.info.numkeys = 1;
    rc = newleaf.SetKey(0, newKey);
    if (rc) {return rc;}
    rc = newleaf.SetVal(0, newValue);
    if (rc) {return rc;}
    rc = newleaf.Serialize(buffercache, leftNode);
    if (rc) {return rc;}
    //now change root
    //So left node takes in keys/values less than OR EQUAL to current key
    originalRoot.info.numkeys = 1;
    rc = originalRoot.SetKey(0, newKey);
    if (rc) {return rc;}
    rc = originalRoot.SetPtr(0, leftNode);
    if (rc) {return rc;}
    rc = originalRoot.SetPtr(1, rightNode);
    if (rc) {return rc;}
    rc = originalRoot.Serialize(buffercache, superblock.info.rootnode);
    if (rc) {return rc;}
    return 0;
  }
  //Else: btree already exists: Must recurse through tree, find leaf to place value and see if splits are needed
  bool didsplit = false;
  SIZE_T left = 0;
  SIZE_T right = 0;
  rc = recurse(superblock.info.rootnode, newKey, newValue, didsplit, left, right);

  return rc;

}

ERROR_T BTreeIndex::Interior_Split(SIZE_T &node, KEY_T &key, SIZE_T &left, SIZE_T &right){

     BTreeNode b;
     ERROR_T rc;
     SIZE_T offset;
     KEY_T currKey;
     // SIZE_T currPtr;
     KEY_T oldKey;
     VALUE_T oldValue;
     SIZE_T oldPtr;
     SIZE_T target;

     rc = b.Unserialize(buffercache, node);

     // Initialize our new node that we want to insert
     BTreeNode newNode = b;

     // Find where we need to split, create the new right node and allocate it
     SIZE_T splitLoc = b.info.numkeys / 2;
     SIZE_T newRightNode;
     rc = AllocateNode(newRightNode);
     if (rc) { return rc; }
     b.GetKey(splitLoc, oldKey);

     // runs if statement if we want to put it inside the left node
     if (key < oldKey) {
       newNode.info.numkeys = b.info.numkeys - splitLoc;
       for (offset = splitLoc; offset < b.info.numkeys; offset++) {
         // Transfer the Key
         rc = b.GetKey(offset, oldKey);
         if (rc) { return rc; }
         rc = newNode.SetKey((offset-splitLoc), oldKey);
         if (rc) { return rc; }
         // Transfer the Ptr
         rc = b.GetPtr(offset, oldPtr);
         if (rc) { return rc; }
         rc = newNode.SetPtr((offset-splitLoc), oldPtr);
         if (rc) { return rc; }
         // update the next Ptr
         rc = b.GetPtr(offset+1, oldPtr);
         if (rc) { return rc; }
         rc = newNode.SetPtr((offset-splitLoc+1), oldPtr);
         if (rc) { return rc; }
       }
       // Insert the new right node
       rc = newNode.Serialize(buffercache, newRightNode);
       if (rc) { return rc; }
       b.info.numkeys = splitLoc;

       // Now we want to add the promoted node from below into the new split
       for (offset = 0; offset < b.info.numkeys; offset++) {
         rc = b.GetKey(offset, currKey);
         if (rc) { return rc; }
         if (key < currKey) {
           break;
         }
       }

       // Now offset is the loc where we want to insert the key,
       // but we need to shift everything after the target location
       target = offset;
       b.info.numkeys += 1;

       for (offset = b.info.numkeys-1; offset > target; offset--) {
         // Shift the key
         rc = b.GetKey(offset-1, oldKey);
         if (rc) { return rc; }
         rc = b.SetKey(offset, oldKey);
         if (rc) { return rc; }
         // Shift the ptr
         rc = b.GetPtr(offset, oldPtr);
         if (rc) { return rc; }
         rc = b.SetPtr(offset+1, oldPtr);
         if (rc) { return rc; }
         rc = b.GetPtr(offset-1, oldPtr);
         if (rc) { return rc; }
         rc = b.SetPtr(offset, oldPtr);
         if (rc) { return rc; }
       }
       // Now that everything after is shifted, insert the new key
       rc = b.SetKey(target, key);
       if (rc) { return rc; }
       rc = b.SetPtr(target+1, right);
       if (rc) { return rc; }
       rc = b.SetPtr(target, left);
       if (rc) { return rc; }

       // We need to account for edge case where inserted key is largest key
       // in the left node
       KEY_T insertKey;
       rc = b.GetKey(b.info.numkeys-1, insertKey);
       if (rc) { return rc; }
       if (insertKey == key) {
         b.SetPtr(b.info.numkeys-1, left);
         newNode.SetPtr(0, right);
         newNode.Serialize(buffercache, newRightNode);
       }
       b.GetKey(b.info.numkeys-1, key);
       left = node;
       right = newRightNode;
       b.info.numkeys--;
       return b.Serialize(buffercache, node);
     } else { // new key should be in the right after the split
       newNode.info.numkeys = b.info.numkeys - splitLoc - 1;
       SIZE_T temp = splitLoc + 1;
       // write the new left node since it's the same
       for (offset = splitLoc+1; offset < b.info.numkeys; offset++) {
         rc = b.GetKey(offset, oldKey);
         if (rc) { return rc; }
         rc = newNode.SetKey((offset-temp), oldKey);
         if (rc) { return rc; }
         rc = b.GetPtr(offset, oldPtr);
         if (rc) { return rc; }
         rc = newNode.SetPtr((offset-temp), oldPtr);
         if (rc) { return rc; }
         rc = b.GetPtr((offset+1), oldPtr);
         if (rc) { return rc; }
         rc = newNode.SetPtr((offset-splitLoc), oldPtr);
         if (rc) { return rc; }
       }
       b.info.numkeys = splitLoc + 1;

       // now insert the key into the newNode
       for (offset = 0; offset < newNode.info.numkeys; offset++) {
         rc = newNode.GetKey(offset, currKey);
         if (rc) { return rc; }
         if (key < currKey) {
           break; // we've found where we want to insert it
         }
       }
       newNode.info.numkeys++;
       target = offset;

       // Like before, we now have to shift everything after it
       for (offset = newNode.info.numkeys-1; offset > target; offset--) {
         rc = newNode.GetKey(offset-1, oldKey);
         if (rc) { return rc; }
         rc = newNode.SetKey(offset, oldKey);
         if (rc) { return rc; }
         rc = newNode.GetPtr(offset, oldPtr);
         if (rc) { return rc; }
         rc = newNode.SetPtr(offset+1, oldPtr);
         if (rc) { return rc; }
         rc = newNode.GetPtr(offset-1, oldPtr);
         if (rc) { return rc; }
         rc = newNode.SetPtr(offset, oldPtr);
         if (rc) { return rc; }
       }
       // Now that everything is properly shifted, insert the new node
       rc = newNode.SetKey(target, key);
       if (rc) { return rc; }
       rc = newNode.SetPtr(target, left);
       if (rc) { return rc; }
       rc = newNode.SetPtr(target+1, right);
       if (rc) { return rc; }

       rc = b.GetKey(b.info.numkeys-1, key);
       if (rc) { return rc; }
       b.info.numkeys--;
       rc = b.Serialize(buffercache, node);
       if (rc) { return rc; }
       right = newRightNode;
       left = node;
       return newNode.Serialize(buffercache, newRightNode);
    }
}


ERROR_T BTreeIndex::Interior_No_Split(SIZE_T &node, KEY_T &key, SIZE_T &left, SIZE_T &right) {
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T currKey;
  KEY_T oldKey;
  SIZE_T oldPtr;
  SIZE_T target;
  rc = b.Unserialize(buffercache, node);

  // Set offset to correct location in block
  for (offset=0; offset<b.info.numkeys; offset++) {
    rc = b.GetKey(offset, currKey);
    if (rc) {return rc;}
    if (key < currKey) {
      break;
    }
  }
  target = offset;
  // Move key/ptr pairs over in block
  b.info.numkeys += 1;
  for (offset=b.info.numkeys-1; offset>target; offset--) {
    rc = b.GetKey(offset-1, oldKey);
    if (rc) {return rc;}
    rc = b.SetKey(offset, oldKey);
    if (rc) {return rc;}
    rc = b.GetPtr(offset, oldPtr);
    if (rc) {return rc;}
    rc = b.SetPtr(offset+1, oldPtr);
    if (rc) {return rc;}
    rc = b.GetPtr(offset-1, oldPtr);
    if (rc) {return rc;}
    rc = b.SetPtr(offset, oldPtr);
    if (rc) {return rc;}
  }
  // Insert key into block
  rc = b.SetKey(target, key);
  if (rc) {return rc;}
  rc = b.SetPtr(target, left);
  if (rc) {return rc;}
  rc = b.SetPtr(target+1, right);
  if (rc) {return rc;}
  return b.Serialize(buffercache, node);
}

ERROR_T BTreeIndex::Root_Split(SIZE_T &node, KEY_T &key, SIZE_T &left, SIZE_T &right) {
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T currKey;
  KEY_T oldKey;
  SIZE_T oldPtr;
  SIZE_T target;
  rc = b.Unserialize(buffercache, node);

  // Create new root node, initialized to old root values
  BTreeNode root = b;
  // Create new interior node
  b.info.nodetype = BTREE_INTERIOR_NODE;
  BTreeNode newNode = b;

  // Get middle of old root, to be split into left and right
  SIZE_T midpoint = b.info.numkeys/2;
  SIZE_T newLeft;
  SIZE_T newRight;
  rc = AllocateNode(newLeft);
  if (rc) {return rc;}
  rc = AllocateNode(newRight);
  if (rc) {return rc;}

  // Find whether to put new key in left or right
  b.GetKey(midpoint, oldKey);

  // Case 1: Key less than oldKey; key goes in left
  if (key < oldKey) {
    newNode.info.numkeys = b.info.numkeys - midpoint;
    // Set up right node: last half of original
    for (offset = midpoint; offset<b.info.numkeys; offset++) {
      rc = b.GetKey(offset, oldKey);
      if (rc) {return rc;}
      rc = newNode.SetKey(offset-midpoint, oldKey);
      if (rc) {return rc;}
      rc = b.GetPtr(offset, oldPtr);
      if (rc) {return rc;}
      rc = newNode.SetPtr(offset-midpoint, oldPtr);
      if (rc) {return rc;}
      rc = b.GetPtr(offset+1, oldPtr);
      if (rc) {return rc;}
      rc = newNode.SetPtr(offset-midpoint+1, oldPtr);
      if (rc) {return rc;}
    }
    rc = newNode.Serialize(buffercache, newRight);
    if (rc) {return rc;}

    // Set up left node: first half of original + promoted key
    newNode.info.numkeys = midpoint;
    // Find location in node to insert key
    for (offset=0; offset<newNode.info.numkeys; offset++) {
      rc = b.GetKey(offset, currKey);
      if (rc) {return rc;}
      if (key < currKey) {
        break;
      }
    }
    target = offset;
    b.info.numkeys += 1;
    for (offset=b.info.numkeys-1; offset>target; offset--) {
      rc = b.GetKey(offset-1, oldKey);
      if (rc) {return rc;}
      rc = b.SetKey(offset, oldKey);
      if (rc) {return rc;}
      rc = b.GetPtr(offset, oldPtr);
      if (rc) {return rc;}
      rc = b.SetPtr(offset+1, oldPtr);
      if (rc) {return rc;}
      rc = b.GetPtr(offset-1, oldPtr);
      if (rc) {return rc;}
      rc = b.SetPtr(offset, oldPtr);
      if (rc) {return rc;}
    }
    // Insert promoted key
    rc = b.SetKey(target, key);
    if (rc) {return rc;}
    rc = b.SetPtr(target, left);
    if (rc) {return rc;}
    rc = b.SetPtr(target+1, right);
    if (rc) {return rc;}
    // Special case: if promoted key is largest in left node (becoming root)
    KEY_T check;
    b.GetKey(b.info.numkeys-1, check);
    if (key == check) {
      newNode.SetPtr(0, right);
      newNode.Serialize(buffercache, newRight);
      b.SetPtr(b.info.numkeys-1, left);
    }

    // Set up new root with key to be promoted
    KEY_T rootkey;
    b.GetKey(b.info.numkeys-1, rootkey);
    b.info.numkeys -= 1;
    rc = b.Serialize(buffercache, newLeft);
    if (rc) {return rc;}
    root.info.numkeys = 1;
    root.SetKey(0, rootkey);
    root.SetPtr(0, newLeft);
    root.SetPtr(1, newRight);
    return root.Serialize(buffercache, superblock.info.rootnode);
  }

  // Case 2: Key goes in right node
  else {
    // Set up right node
    // Move key/ptr pairs over
    newNode.info.numkeys = b.info.numkeys - midpoint - 1;
    for (offset=midpoint+1; offset<b.info.numkeys; offset++) {
      rc = b.GetKey(offset, oldKey);
      if (rc) {return rc;}
      rc = newNode.SetKey(offset-midpoint-1, oldKey);
      if (rc) {return rc;}
      rc = b.GetPtr(offset, oldPtr);
      if (rc) {return rc;}
      rc = newNode.SetPtr(offset-midpoint-1, oldPtr);
      if (rc) {return rc;}
      rc = b.GetPtr(offset+1, oldPtr);
      if (rc) {return rc;}
      rc = newNode.SetPtr(offset-midpoint, oldPtr);
      if (rc) {return rc;}
    }
    // Find location in node to insert key
    for (offset=0; offset<newNode.info.numkeys; offset++) {
      rc = newNode.GetKey(offset, currKey);
      if (rc) {return rc;}
      if (key < currKey) {
        break;
      }
    }
    target = offset;
    // Update node, shift to make room for inserted key
    newNode.info.numkeys += 1;
    for (offset=newNode.info.numkeys-1; offset>target; offset--) {
      rc = newNode.GetKey(offset-1, oldKey);
      if (rc) {return rc;}
      rc = newNode.SetKey(offset, oldKey);
      if (rc) {return rc;}
      rc = newNode.GetPtr(offset, oldPtr);
      if (rc) {return rc;}
      rc = newNode.SetPtr(offset+1, oldPtr);
      if (rc) {return rc;}
      rc = newNode.GetPtr(offset-1, oldPtr);
      if (rc) {return rc;}
      rc = newNode.SetPtr(offset, oldPtr);
      if (rc) {return rc;}
    }
    // Insert key
    rc = newNode.SetKey(target, key);
    rc = newNode.SetPtr(target+1, right);
    if (rc) {return rc;}
    rc = newNode.SetPtr(target, left);
    if (rc) {return rc;}
    // Serialize right node
    rc = newNode.Serialize(buffercache, newRight);
    if (rc) {return rc;}

    // Set up left node: first half of original node
    b.info.numkeys = midpoint + 1;
    // Get key to promote
    KEY_T rootkey;
    b.GetKey(b.info.numkeys-1, rootkey);
    b.info.numkeys -= 1;
    rc = b.Serialize(buffercache, newLeft);

    // Update root
    root.info.numkeys = 1;
    root.SetKey(0, rootkey);
    root.SetPtr(0, newLeft);
    root.SetPtr(1, newRight);
    return root.Serialize(buffercache, superblock.info.rootnode);
  }
  return ERROR_INSANE;
}

ERROR_T BTreeIndex::recurse(SIZE_T &node, KEY_T &key, VALUE_T &value, bool &split, SIZE_T &left, SIZE_T &right){
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  SIZE_T currPtr;
  SIZE_T oldPtr;
  KEY_T currKey;
  KEY_T oldKey;
  VALUE_T oldValue;
  SIZE_T insertIndex;

  bool found = false;
  SIZE_T paramNode = node;
  SIZE_T paramLeft = left;
  SIZE_T paramRight = right;
  KEY_T paramKey = key;



  rc = b.Unserialize(buffercache,node);
  if(rc!=ERROR_NOERROR){
    return rc;
  }
  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
    if(b.info.numkeys <= 0){
      return ERROR_INSANE;
    }
    for(offset = 0; offset<b.info.numkeys;offset++){
      rc = b.GetKey(offset, currKey);
      if(rc){
        return rc;
      }
      if(key < currKey || currKey == key){
        rc=b.GetPtr(offset, currPtr);
        if(rc){
          return rc;
        }
        rc = recurse(currPtr, key, value, split, left, right);
        if(rc){
          return rc;
        }
        found = true;
        break;

      }
    }//end of iterating through keys at currentnode
    //Examine last pointer in case appropriate key not found. Because last choice, must take it
    if(found == false){
      rc = b.GetPtr(b.info.numkeys, currPtr);
      if(rc){
        return rc;
      }
      rc = recurse(currPtr, key, value, split, left, right);
      if(rc){
        return rc;
      }
    }
    //After recursing, split = 0 or 1
    //check here for split, otherwise just add key/val and done SPLIT?
    if(split){
      //do stuff
      //Cases: interior node dealing with leaf split
      //interior node deals with leaf split and then must split itself as well
      //root node must split because interior node split
      //root node deals with interior split but is not full

      //set split = 0 when split does not happen: means we can stop. New splits can cause more splits, however.
      //if currently looking at int node after a split
      if(b.info.nodetype == BTREE_INTERIOR_NODE){
        if(b.info.numkeys < b.info.GetNumSlotsAsLeaf()){ //if interior node is not full
          paramNode = node;
          paramKey = key;
          paramLeft = left;
          paramRight = right;
          rc = Interior_No_Split(paramNode, paramKey, paramLeft, paramRight);
          node = paramNode;
          key = paramKey;
          left = paramLeft;
          right = paramRight;
          //Interior_No_Split
          //Interior_Split
          //Root_Split
          split = 0;
          return rc;
        }
        else{ //must split interior node as well
          paramNode = node;
          paramKey = key;
          paramLeft = left;
          paramRight = right;
          rc = Interior_Split(paramNode, paramKey, paramLeft, paramRight);
          node = paramNode;
          key = paramKey;
          left = paramLeft;
          right = paramRight;
          split = 1;
          return rc;
        }
      }
      else{ //Looking at root node immediately after splitting something
        if(b.info.numkeys < b.info.GetNumSlotsAsLeaf()){ //root is not full
          paramNode = node;
          paramKey = key;
          paramLeft = left;
          paramRight = right;
          rc = Interior_No_Split(paramNode, paramKey, paramLeft, paramRight);
          node = paramNode;
          key = paramKey;
          left = paramLeft;
          right = paramRight;
          split = 0;
          return rc;
        }
        else{ //root is full, must split root.
          paramNode = node;
          paramKey = key;
          paramLeft = left;
          paramRight = right;
          rc = Root_Split(paramNode, paramKey, paramLeft, paramRight);
          split = 0; //Root was split, but no function call to handle so no point in setting to 1
          return rc;
        }
      }
    }
    else{
      return rc;
    }
    break;

    case BTREE_LEAF_NODE:
    //leaf not full
    if(b.info.numkeys < b.info.GetNumSlotsAsLeaf()){
      for(offset = 0; offset<b.info.numkeys; offset++){
        rc = b.GetKey(offset, currKey);
        if(rc){
          return rc;
        }
        if(key < currKey){ //we found the first key node that our insert key is greater than
          break;
        }
      }
      insertIndex = offset;
      b.info.numkeys = b.info.numkeys + 1;
      for(offset = b.info.numkeys - 1; offset > insertIndex; offset--){
        rc = b.GetKey(offset-1,oldKey); //oldKey holding value for now to move it later
        if(rc){
          return rc;
        }
        rc = b.GetVal(offset - 1, oldValue);
        if(rc){
          return rc;
        }
        rc = b.SetKey(offset, oldKey);
        if(rc){
          return rc;
        }
        rc = b.SetVal(offset, oldValue);
        if(rc){
          return rc;
        }
      }
      rc = b.SetVal(insertIndex, value); //insert value
      if(rc){
        return rc;
      }
      rc = b.SetKey(insertIndex, key); //insert key
      if(rc){
        return rc;
      }
      return b.Serialize(buffercache, node);
      break;
    }
    else{ //leaf node is full, have to do a split
      split = 1; //Telling previous level about split, so can add key to previous level
      left = node; //current node becomes location of left pointer (original)
      rc = AllocateNode(right); //right is new node
      if(rc){
        return rc;
      }
      BTreeNode newNode = b;
      SIZE_T mid = b.info.numkeys / 2;
      rc = b.GetKey(mid, oldKey); //key value at middle, see if new key is before or after
      if(rc){return rc;}
      if(key < oldKey){ //before
        newNode.info.numkeys = b.info.numkeys - mid;
        for(offset = mid; offset < b.info.numkeys; offset++){ //create right node and serialize
          rc = b.GetKey(offset, oldKey);
          if(rc){
            return rc;
          }
          rc = b.GetVal(offset, oldValue);
          if(rc){
            return rc;
          }
          rc = newNode.SetKey(offset - mid, oldKey); //offset - mid to give starting indices of new node
          if(rc){
            return rc;
          }
          rc = newNode.SetVal(offset - mid, oldValue);
          if(rc){
            return rc;
          }
        }
        rc = newNode.Serialize(buffercache, right);
        if(rc){
          return rc;
        }
        //Now insert into left half
        b.info.numkeys = mid; //numkeys cut down
        for(offset = 0; offset < b.info.numkeys; offset++){
          rc = b.GetKey(offset, currKey);
          if(rc){
            return rc;
          }
          if(key < currKey){ //found first key greater than insert key
            break;
          }
        }
        insertIndex = offset;
        b.info.numkeys++; //adding another key
        for(offset = b.info.numkeys - 1; offset > insertIndex; offset--){//copy elements forward
          rc = b.GetKey(offset-1, oldKey);
          if(rc){
            return rc;
          }
          rc = b.GetVal(offset-1, oldValue);
          if(rc){
            return rc;
          }
          rc = b.SetKey(offset, oldKey);
          if(rc){
            return rc;
          }
          rc = b.SetVal(offset, oldValue);
          if(rc){
            return rc;
          }
        }
        rc = b.SetVal(insertIndex, value);
        if(rc){
          return rc;
        }
        rc = b.SetKey(insertIndex, key);
        if(rc){
          return rc;
        }
        b.GetKey(b.info.numkeys - 1, key); //modify global key because inserted orignal value, not needed anymore
        return b.Serialize(buffercache,left);
      }
      else{//2nd half
        newNode.info.numkeys = b.info.numkeys - mid - 1;
        for(offset = mid + 1; offset < b.info.numkeys; offset++){ //copy right half to right node
          rc = b.GetKey(offset, oldKey);
          if(rc){
            return rc;
          }
          rc = b.GetVal(offset, oldValue);
          if(rc){
            return rc;
          }
          rc = newNode.SetKey(offset - mid - 1, oldKey);
          if(rc){
            return rc;
          }
          rc = newNode.SetVal(offset - mid - 1, oldValue);
          if(rc){
            return rc;
          }
        }
        b.info.numkeys = mid + 1; //restrict left side keys to split
        rc = b.Serialize(buffercache, left);
        if(rc){
          return rc;
        }//now insert into right node
        for(offset = 0; offset < newNode.info.numkeys;offset++){
          rc = newNode.GetKey(offset, currKey);
          if(rc){
            return rc;
          }
          if(key < currKey){
            break;
          }
        }
        insertIndex = offset;
        newNode.info.numkeys += 1; //adding new key
        for(offset = newNode.info.numkeys - 1; offset > insertIndex; offset--){
          rc = newNode.GetKey(offset-1, oldKey);
          if(rc){
            return rc;
          }
          rc = newNode.GetVal(offset-1, oldValue);
          if(rc){
            return rc;
          }
          rc = newNode.SetKey(offset, oldKey);
          if(rc){
            return rc;
          }
          rc = newNode.SetVal(offset, oldValue);
          if(rc){
            return rc;
          }
        }
        rc = newNode.SetVal(insertIndex, value);
        if(rc){
          return rc;
        }
        rc = newNode.SetKey(insertIndex, key);
        if(rc){




          return rc;
        }
        b.GetKey(b.info.numkeys-1, key); //changing key to greatest val in left
        return newNode.Serialize(buffercache, right);

      }
    }
    default:
    return ERROR_INSANE;
    break;


  }//end of switch statement
return ERROR_INSANE;
}



ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  VALUE_T x = value;
  if(superblock.info.valuesize != value.length){
    return ERROR_SIZE;
  }
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, x);
}


ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit
  //
  //
  return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);

  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) {
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) {
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) {
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) {
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) {
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) {
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  BTreeNode root;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T currKey;
  SIZE_T currPtr;
  KEY_T prevKey;
  rc = root.Unserialize(buffercache, superblock.info.rootnode);
  if(rc){return rc;}
  rc = root.GetKey(0, prevKey);
  if(rc){return rc;}
  for(offset = 1; offset<root.info.numkeys;offset++){
    rc = root.GetKey(offset, currKey);
    if(rc){return rc;}
    rc = root.GetPtr(offset, currPtr);
    if(rc){return rc;}
    if(prevKey == currKey || !(prevKey < currKey)){
      return ERROR_BADCONFIG;
    }
    rc = root.GetKey(offset, prevKey);
    if(rc){return rc;}
  }
  for(offset = 0; offset < root.info.numkeys; offset++){
    rc = root.GetPtr(offset, currPtr);
    if(rc){return rc;}
    rc = SanityDfs(currPtr);

  }

  return ERROR_NOERROR;


}
ERROR_T BTreeIndex::SanityDfs(SIZE_T &node) const
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T currKey;
  SIZE_T currPtr;
  KEY_T prevKey;

  rc = b.Unserialize(buffercache, node);
  if(rc){return rc;}

  if(b.info.numkeys == 0){
    return ERROR_NOERROR;
  }
  rc = b.GetKey(0, prevKey);
  if(rc){return rc;}
  for(offset = 1; offset<b.info.numkeys;offset++){
    rc = b.GetKey(offset, currKey);
    if(rc){return rc;}
    rc = b.GetPtr(offset, currPtr);
    if(rc){return rc;}
    if(prevKey == currKey || !(prevKey < currKey)){
      return ERROR_BADCONFIG;
    }
    rc = b.GetKey(offset, prevKey);
    if(rc){return rc;}
  }
  for(offset = 0; offset < b.info.numkeys; offset++){
    rc = b.GetPtr(offset, currPtr);
    if(rc){return rc;}
    return SanityDfs(currPtr);
  }
}


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  Display(os, BTREE_DEPTH_DOT);
  return os;
}
