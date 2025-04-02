use anyhow::Result;
use certify::{generate_ca, generate_cert, CertSigAlgo, CertType, CA};
use tokio::fs;

struct CertPem {
    cert_type: CertType,
    cert: String,
    key: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let pem = creat_ca()?;
    gen_files(&pem).await?;
    let ca = CA::load(&pem.cert, &pem.key)?;
    // Generate a cert and key for a server
    let pem = creat_cert(&ca, &["kvserver.acme.inc"], "Acme KV server", false)?;
    gen_files(&pem).await?;
    // Generate a cert and key for a client
    let pem = creat_cert(&ca, &[], "awesome-device-id", true)?;
    gen_files(&pem).await?;

    Ok(())
}

fn creat_ca() -> Result<CertPem> {
    let (cert, key) = generate_ca(
        "CN",                 // country code
        "Acme Inc",           // organization name
        "Acme CA",            // CA name
        CertSigAlgo::ED25519, // signature algorithm
        None,                 // extended key usage(Optional)
        Some(10 * 365),       // validity days
    )?;

    Ok(CertPem {
        cert_type: CertType::CA,
        cert,
        key,
    })
}

/// Generate a cert and key for a server or client
fn creat_cert(ca: &CA, domains: &[&str], cn: &str, is_client: bool) -> Result<CertPem> {
    let (days, cert_type) = if is_client {
        (Some(365), CertType::Client)
    } else {
        (Some(5 * 365), CertType::Server)
    };

    let (cert, key) = generate_cert(
        ca,
        domains.to_vec(),
        "CN",
        "Acme Inc",
        cn,
        CertSigAlgo::ED25519,
        None,
        is_client,
        days,
    )?;

    Ok(CertPem {
        cert_type,
        cert,
        key,
    })
}

async fn gen_files(pem: &CertPem) -> Result<()> {
    let name = match pem.cert_type {
        CertType::CA => "ca",
        CertType::Client => "client",
        CertType::Server => "server",
    };
    fs::write(format!("fixtures/{}.cert", name), pem.cert.as_bytes()).await?;
    fs::write(format!("fixtures/{}.key", name), pem.key.as_bytes()).await?;

    Ok(())
}
