//! `zou map <target> …`: inspect and edit the cell's shard map.
//!
//! The map is one versioned object at `ctl/MAP`, the node roster plus
//! the pins that override rendezvous placement for heat balancing.
//! Every edit is one CAS publish that bumps the version, and the data
//! plane picks the change up through the version handshake on its next
//! rpc, no restart anywhere.

use zou_store::placement::{self, Pin};
use zou_store::{Node, open_store};

pub const USAGE: &str = "usage: zou map <target> show | nodes <id=addr>… | \
     pin <ref> <shard> <node> | unpin <ref> <shard> | place <ref> <shard>";

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, rest) = match argv {
        [target, rest @ ..] if !rest.is_empty() => (target, rest),
        _ => return Err(USAGE.into()),
    };
    let store = open_store(target)?;
    match rest {
        [verb] if verb == "show" => {
            let map = placement::load(&*store).map_err(|e| e.to_string())?;
            print!("{}", String::from_utf8_lossy(&map.to_json()));
            Ok(())
        }
        [verb, entries @ ..] if verb == "nodes" && !entries.is_empty() => {
            let nodes = entries
                .iter()
                .map(|e| {
                    let (id, addr) = e.split_once('=').ok_or(USAGE)?;
                    Ok(Node {
                        id: id.to_string(),
                        addr: addr.to_string(),
                    })
                })
                .collect::<Result<Vec<_>, &str>>()?;
            let map = placement::publish(&*store, |m| m.nodes = nodes.clone())
                .map_err(|e| e.to_string())?;
            println!("map version {} with {} nodes", map.version, map.nodes.len());
            Ok(())
        }
        [verb, tenant_ref, shard, node] if verb == "pin" => {
            let shard: u16 = shard.parse().map_err(|_| USAGE.to_string())?;
            let pin = Pin {
                tenant: tenant_ref.to_string(),
                shard,
                node: node.to_string(),
            };
            let map = placement::publish(&*store, |m| {
                m.pins
                    .retain(|p| (&p.tenant, p.shard) != (&pin.tenant, shard));
                m.pins.push(pin.clone());
            })
            .map_err(|e| e.to_string())?;
            println!(
                "map version {}: {tenant_ref} shard {shard} pinned to {node}",
                map.version
            );
            Ok(())
        }
        [verb, tenant_ref, shard] if verb == "unpin" => {
            let shard: u16 = shard.parse().map_err(|_| USAGE.to_string())?;
            let map = placement::publish(&*store, |m| {
                m.pins
                    .retain(|p| !(p.tenant == *tenant_ref && p.shard == shard));
            })
            .map_err(|e| e.to_string())?;
            println!(
                "map version {}: {tenant_ref} shard {shard} unpinned",
                map.version
            );
            Ok(())
        }
        [verb, tenant_ref, shard] if verb == "place" => {
            let shard: u16 = shard.parse().map_err(|_| USAGE.to_string())?;
            let map = placement::load(&*store).map_err(|e| e.to_string())?;
            let owner = map
                .node_for(tenant_ref, shard)
                .ok_or("the map has no nodes")?;
            println!("{} {}", owner.id, owner.addr);
            for standby in map.rank(tenant_ref, shard).iter().skip(1) {
                println!("  standby {} {}", standby.id, standby.addr);
            }
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}
