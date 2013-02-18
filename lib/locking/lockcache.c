#include "lib.h"
#include "lockcache.h"
#include "device.h"
#include "metadata-exported.h"

static struct dm_hash_table *_lock_hash = NULL;
static int _vgs_locked = 0;

static void _lockcache_destroy_lockname(struct dm_hash_node *n)
{
	char *vgname;

	if (!dm_hash_get_data(_lock_hash, n))
		return;

	vgname = dm_hash_get_key(_lock_hash, n);

	if (strcmp(vgname, VG_GLOBAL))
		log_error(INTERNAL_ERROR "Volume Group %s was not unlocked",
			  dm_hash_get_key(_lock_hash, n));
}

static int _lockcache_init(void)
{
	if (_lock_hash)
		return 1;

	/*
	 * FIXME add a proper lockcache_reset() that resets the cache so no
	 * previous locks are locked
	 */

	_vgs_locked = 0;

	if (!(_lock_hash = dm_hash_create(128)))
		return 0;
	return 1;

}

void lockcache_destroy(void) {
	struct dm_hash_node *n;
	if (_lock_hash) {
		dm_hash_iterate(n, _lock_hash)
			_lockcache_destroy_lockname(n);
		dm_hash_destroy(_lock_hash);
		_lock_hash = NULL;
	}
}

int lockcache_vgname_is_locked(const char *vgname)
{
	if (!_lock_hash)
		return 0;

	return dm_hash_lookup(_lock_hash, is_orphan_vg(vgname) ? VG_ORPHANS : vgname) ? 1 : 0;
}

void lockcache_lock_vgname(const char *vgname, int read_only __attribute__((unused)))
{
	if (!_lockcache_init()) {
		log_error("Internal cache initialisation failed");
		return;
	}

	if (dm_hash_lookup(_lock_hash, vgname))
		log_error(INTERNAL_ERROR "Nested locking attempted on VG %s.",
			  vgname);

	if (!dm_hash_insert(_lock_hash, vgname, (void *) 1))
		log_error("Cache locking failure for %s", vgname);

	if (strcmp(vgname, VG_GLOBAL))
		_vgs_locked++;
}


void lockcache_unlock_vgname(const char *vgname)
{
	if (!_lockcache_init()) {
		log_error("Internal cache initialisation failed");
		return;
	}

	if (!dm_hash_lookup(_lock_hash, vgname))
		log_error(INTERNAL_ERROR "Attempt to unlock unlocked VG %s.",
			  vgname);

	dm_hash_remove(_lock_hash, vgname);

	/* FIXME Do this per-VG */
	if (strcmp(vgname, VG_GLOBAL) && !--_vgs_locked)
		dev_close_all();
}

int lockcache_vgs_locked(void)
{
	return _vgs_locked;
}

/*
 * Ensure vgname2 comes after vgname1 alphabetically.
 * Orphan locks come last.
 * VG_GLOBAL comes first.
 */
static int _vgname_order_correct(const char *vgname1, const char *vgname2)
{
	if (is_global_vg(vgname1))
		return 1;

	if (is_global_vg(vgname2))
		return 0;

	if (is_orphan_vg(vgname1))
		return 0;

	if (is_orphan_vg(vgname2))
		return 1;

	if (strcmp(vgname1, vgname2) < 0)
		return 1;

	return 0;
}

/*
 * Ensure VG locks are acquired in alphabetical order.
 */
int lockcache_verify_lock_order(const char *vgname)
{
	struct dm_hash_node *n;
	const char *vgname2;

	if (!_lockcache_init())
		return_0;

	dm_hash_iterate(n, _lock_hash) {
		if (!dm_hash_get_data(_lock_hash, n))
			return_0;

		if (!(vgname2 = dm_hash_get_key(_lock_hash, n))) {
			log_error(INTERNAL_ERROR "VG lock %s hits NULL.",
				 vgname);
			return 0;
		}

		if (!_vgname_order_correct(vgname2, vgname)) {
			log_errno(EDEADLK, INTERNAL_ERROR "VG lock %s must "
				  "be requested before %s, not after.",
				  vgname, vgname2);
			return 0;
		}
	}

	return 1;
}
