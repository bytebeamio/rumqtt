use vergen::{vergen, Config};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate the default 'cargo:' instruction output
    vergen(Config::default())?;
    Ok(())
}
